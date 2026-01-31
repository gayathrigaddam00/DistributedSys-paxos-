package com.example.paxos.core;

import com.example.paxos.v1.*;
import com.google.protobuf.Empty;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class PaxosCore {

    public record NewViewResult(NewViewReq req, long maxSeq) {}

    private final String nodeId;
    private final Set<String> allNodeIds;
    private final int quorum;
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    private Ballot highestSeen = Ballot.newBuilder().setEpoch(0).setLeaderId("").build();
    private Ballot currentLeader = null;

    private final AtomicLong nextSeq = new AtomicLong(1);
    private final ConcurrentMap<Long, AcceptedEntry> acceptLog = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, Value> committed = new ConcurrentHashMap<>();
    private final AtomicLong executedUpTo = new AtomicLong(0);

    private final ConcurrentMap<Long, Set<String>> acksBySeq = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> db = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ClientReply> lastReplyByClient = new ConcurrentHashMap<>();
    public enum Status { X, A, C, E }
    private final ConcurrentMap<Long, ConcurrentMap<String, Status>> statusBySeq = new ConcurrentHashMap<>();
    private final List<NewViewReq> viewHistory = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<Long, PendingClient> pendingClientBySeq = new ConcurrentHashMap<>();
    private final List<String> messageHistoryLog = new CopyOnWriteArrayList<>();

    public static final class Broadcast {
        public final List<Object> messages;
        public Broadcast(List<Object> messages) { this.messages = messages; }
    }

    public static final class ReplyAction {
        public final ClientReply reply;
        public ReplyAction(ClientReply reply) { this.reply = reply; }
    }

    private record PendingClient(String clientId, long clientTs) {}

    public PaxosCore(String nodeId, Set<String> allNodeIds, Map<String, Integer> initialBalances) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.allNodeIds = new HashSet<>(Objects.requireNonNull(allNodeIds));
        if (!this.allNodeIds.contains(nodeId)) throw new IllegalArgumentException("allNodeIds must include self");
        this.quorum = (this.allNodeIds.size() / 2) + 1;

        if (initialBalances != null && !initialBalances.isEmpty()) {
            db.putAll(initialBalances);
        } else {
            for (char c='A'; c < 'A' + 10; c++) db.put(String.valueOf(c), 10);
        }
    }
    
    public void setActive(boolean status) { this.isActive.set(status); }
    public boolean isActive() { return this.isActive.get(); }

    public synchronized Object onClientSubmit(Txn txn) {
        if (!isActive()) {
        return new ReplyAction(ClientReply.newBuilder()
                .setB(nullSafeBallot(currentLeader))
                .setClientId(txn.getClientId())
                .setClientTs(txn.getClientTs())
                .setSuccess(false)
                .setMessage("NODE_INACTIVE") 
                .build());
    }
        String clientId = txn.getClientId();
        long clientTs = txn.getClientTs();
        ClientReply last = lastReplyByClient.get(clientId);
        if (last != null && last.getClientTs() >= clientTs) {
            return new ReplyAction(last);
        }
        if (!isLeader()) {
            return new ReplyAction(ClientReply.newBuilder()
                    .setB(nullSafeBallot(currentLeader))
                    .setClientId(clientId)
                    .setClientTs(clientTs)
                    .setSuccess(false)
                    .setMessage("NOT_LEADER")
                    .build());
        }
        long s = nextSeq.getAndIncrement();
        Value value = Value.newBuilder().setTxn(txn).build();
        AcceptedEntry accepted = AcceptedEntry.newBuilder().setB(currentLeader).setS(Seq.newBuilder().setNum(s)).setValue(value).build();
        acceptLog.put(s, accepted);
        markStatus(s, nodeId, Status.A);
        pendingClientBySeq.put(s, new PendingClient(clientId, clientTs));
        AcceptReq acceptReq = AcceptReq.newBuilder().setB(currentLeader).setS(Seq.newBuilder().setNum(s)).setValue(value).build();
        acksBySeq.computeIfAbsent(s, k -> new HashSet<>()).add(nodeId);
        return new Broadcast(List.of(acceptReq));
    }

    public synchronized PromiseRes onPrepare(PrepareReq req) {
        Ballot incoming = req.getB();
        if (isHigher(incoming, highestSeen)) {
            highestSeen = incoming;
        }
        List<AcceptedEntry> log = new ArrayList<>(acceptLog.values());
        return PromiseRes.newBuilder().setB(highestSeen).addAllAcceptLog(log).build();
    }

    public synchronized Object onAccept(AcceptReq req) {
        Ballot b = req.getB();
        long seqNum = req.getS().getNum();
        
        Map<String, Status> statusMap = statusBySeq.get(seqNum);
        if (statusMap != null) {
            Status currentStatus = statusMap.get(nodeId);
            if (currentStatus == Status.C || currentStatus == Status.E) {
                return AcceptedReq.newBuilder().setB(b).setS(req.getS()).setFromNodeId(nodeId).build();
            }
        }

        if (isHigher(highestSeen, b)) {
            return Empty.getDefaultInstance();
        }
        if (isHigher(b, currentLeader)) {
            currentLeader = b;
        }
        highestSeen = b;

        AcceptedEntry entry = AcceptedEntry.newBuilder().setB(b).setS(req.getS()).setValue(req.getValue()).build();
        acceptLog.put(seqNum, entry);
        markStatus(seqNum, nodeId, Status.A);
        
        return AcceptedReq.newBuilder().setB(b).setS(req.getS()).setFromNodeId(nodeId).build();
    }

    
    public synchronized Object onAccepted(AcceptedReq req) {
        long s = req.getS().getNum();
        Ballot b = req.getB();
        if (currentLeader == null || !currentLeader.equals(b)) {
            return Empty.getDefaultInstance();
        }
        Set<String> acks = acksBySeq.computeIfAbsent(s, k -> new HashSet<>());
        acks.add(req.getFromNodeId());

        if (acks.size() >= quorum && !committed.containsKey(s)) {
            AcceptedEntry accepted = acceptLog.get(s);
            if (accepted == null) return Empty.getDefaultInstance();

            CommitReq commit = CommitReq.newBuilder().setB(b).setS(accepted.getS()).setValue(accepted.getValue()).build();
            
            onCommit(commit);
            
            return new Broadcast(List.of(commit));
        }
        return Empty.getDefaultInstance();
    }

    public synchronized void onCommit(CommitReq req) {
        if (isHigher(req.getB(), currentLeader)) {
            currentLeader = req.getB();
        }
        committed.putIfAbsent(req.getS().getNum(), req.getValue());
        markStatus(req.getS().getNum(), nodeId, Status.C);
        executeInOrder();
    }
    
    public synchronized void onNewView(NewViewReq req) {
        if(isHigher(req.getB(), currentLeader)) {
            currentLeader = req.getB();
        }
        viewHistory.add(req);
        for (AcceptReq a : req.getRebroadcastBatchList()) {
            long seqNum = a.getS().getNum();
            
            AcceptedEntry entry = AcceptedEntry.newBuilder().setB(a.getB()).setS(a.getS()).setValue(a.getValue()).build();
            acceptLog.put(seqNum, entry);
            markStatus(seqNum, nodeId, Status.A);

            if (!committed.containsKey(seqNum)) {
                 committed.put(seqNum, a.getValue());
                 markStatus(seqNum, nodeId, Status.C);
            }
        }
        
        executeInOrder();
    }

    private void executeInOrder() {
        long next = executedUpTo.get() + 1;
        while (committed.containsKey(next)) {
            if (executedUpTo.get() < next) {
                apply(committed.get(next));
                executedUpTo.set(next);
                markStatus(next, nodeId, Status.E);
            }

            PendingClient pc = pendingClientBySeq.remove(next);
            if (pc != null && isLeader()) {
                ClientReply r = ClientReply.newBuilder()
                        .setB(currentLeader)
                        .setClientId(pc.clientId())
                        .setClientTs(pc.clientTs())
                        .setSuccess(true)
                        .setMessage("COMMITTED")
                        .setSeq(Seq.newBuilder().setNum(next))
                        .build();
                lastReplyByClient.put(pc.clientId(), r);
            }
            next++;
        }
    }

    private void apply(Value v) {
        if (v.hasTxn()) {
            Txn t = v.getTxn();
            int fromBal = db.getOrDefault(t.getFrom(), 0);
            if (fromBal >= t.getAmount()) {
                db.put(t.getFrom(), fromBal - t.getAmount());
                db.put(t.getTo(), db.getOrDefault(t.getTo(), 0) + t.getAmount());
            }
        }
    }

    public synchronized PrepareReq buildPrepare(long newEpoch) {
        Ballot b = Ballot.newBuilder().setEpoch(newEpoch).setLeaderId(nodeId).build();
        return PrepareReq.newBuilder().setB(b).build();
    }

    public synchronized NewViewResult buildNewViewFromPromises(Ballot b, List<PromiseRes> promises) {
        Map<Long, AcceptedEntry> highestBySeq = new HashMap<>();
        long maxSeq = 0;
        for (PromiseRes p : promises) {
            for (AcceptedEntry e : p.getAcceptLogList()) {
                long s = e.getS().getNum();
                maxSeq = Math.max(maxSeq, s);
                AcceptedEntry prev = highestBySeq.get(s);
                if (prev == null || isHigher(e.getB(), prev.getB())) {
                    highestBySeq.put(s, e);
                }
            }
        }
        List<AcceptReq> batch = new ArrayList<>();
        for (long s = 1; s <= maxSeq; s++) {
            AcceptedEntry e = highestBySeq.get(s);
            Value v = (e != null) ? e.getValue() : Value.newBuilder().setNoop(Noop.getDefaultInstance()).build();
            batch.add(AcceptReq.newBuilder().setB(b).setS(Seq.newBuilder().setNum(s)).setValue(v).build());
        }
        NewViewReq req = NewViewReq.newBuilder().setB(b).addAllRebroadcastBatch(batch).build();
        return new NewViewResult(req, maxSeq);
    }
    
    public synchronized void updateSeqAfterElection(long maxSeqSeen) {
        long currentNextSeq = this.nextSeq.get();
        if (maxSeqSeen + 1 > currentNextSeq) {
            this.nextSeq.set(maxSeqSeen + 1);
        }
    }

    public synchronized void becomeLeader(Ballot b) {
        this.currentLeader = b;
        this.highestSeen = b;
    }

    private void markStatus(long seq, String node, Status st) {
        statusBySeq.computeIfAbsent(seq, k -> new ConcurrentHashMap<>()).put(node, st);
    }
    
    private static boolean isHigher(Ballot a, Ballot b) {
        if (a == null || b == null) return a != null;
        if (a.getEpoch() != b.getEpoch()) return a.getEpoch() > b.getEpoch();
        return a.getLeaderId().compareTo(b.getLeaderId()) > 0;
    }

    private static Ballot nullSafeBallot(Ballot b) {
        return b == null ? Ballot.newBuilder().build() : b;
    }

    public boolean isLeader() {
        return currentLeader != null && nodeId.equals(currentLeader.getLeaderId());
    }
    
    public boolean hasLeader() {
        return this.currentLeader != null && !this.currentLeader.getLeaderId().isEmpty();
    }

    private void logMessage(String direction, Object message) {
        String logEntry = String.format("[%s] %s: %s", direction, message.getClass().getSimpleName(), message.toString().replaceAll("\\s+", " "));
        messageHistoryLog.add(logEntry);
    }
    public void logReceived(Object message) { logMessage("RECV", message); }
    public void logSent(Object message) { logMessage("SENT", message); }
    
    public Map<String, Integer> printDB() { return Map.copyOf(db); }
    public List<String> printLog() { return List.copyOf(messageHistoryLog); }
    public Map<String, Status> printStatus(long seq) { ConcurrentMap<String, Status> statusMap = statusBySeq.get(seq); return statusMap != null ? Map.copyOf(statusMap) : Map.of(); }
    public List<NewViewReq> printView() { return List.copyOf(viewHistory); }
    public ClientReply getCachedReply(String clientId) { return lastReplyByClient.get(clientId); }
    public Ballot getCurrentLeader() { return currentLeader; }
    public Ballot getHighestSeen() { return highestSeen; }
}