package com.example.paxos.election;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.v1.*;
import io.grpc.Deadline;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ElectionDriver implements DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(ElectionDriver.class);

    private final PaxosCore core;
    private final String nodeId;
    private final Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peers;
    private final long rpcDeadlineMs;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicLong lastLeaderActivity = new AtomicLong(System.currentTimeMillis());

    private final long leaderTimeoutMs;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean timersRunning = new AtomicBoolean(false);

    public ElectionDriver(
            PaxosCore core,
            @Value("${paxos.nodeId}") String nodeId,
            Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peers,
            @Value("${paxos.rpc.deadlineMs:500}") long rpcDeadlineMs,
            @Value("${paxos.leaderTimeoutMs:1500}") long leaderTimeoutMs
    ) {
        this.core = core;
        this.nodeId = nodeId;
        this.peers = peers;
        this.rpcDeadlineMs = rpcDeadlineMs;
        this.leaderTimeoutMs = leaderTimeoutMs;
    }

   public void startTimers() {
        if (timersRunning.compareAndSet(false, true)) {
            log.info("[{}] Election timers started.", nodeId);
            scheduler.scheduleAtFixedRate(this::checkLeaderHealth, leaderTimeoutMs, leaderTimeoutMs / 2, TimeUnit.MILLISECONDS);
        }
    }

    public void stopTimers() {
        if (timersRunning.compareAndSet(true, false)) {
            log.info("[{}] Election timers stopped.", nodeId);
            scheduler.shutdownNow();
        }
    }

    private void checkLeaderHealth() {
        if (!timersRunning.get() || !core.isActive() || core.isLeader()) {
            return;
        }
        if ((System.currentTimeMillis() - lastLeaderActivity.get()) > leaderTimeoutMs) {
            log.warn("[{}] Leader inactivity detected. Starting election.", nodeId);
            startElection();
        }
    }

    public void recordLeaderActivity() {
        lastLeaderActivity.set(System.currentTimeMillis());
    }

    public synchronized void startElection() {
        if (!core.isActive()) {
            log.warn("[{}] Ignoring election request because node is inactive.", nodeId);
            return;
        }
        if (!electionInProgress.compareAndSet(false, true)) {
            log.info("[{}] Election already in progress, skipping.", nodeId);
            return;
        }

        try {
            long newEpoch = core.getHighestSeen().getEpoch() + 1;
            PrepareReq prepare = core.buildPrepare(newEpoch);
            Ballot b = prepare.getB();

            log.info("[{}] No leader detected. Starting election, sending PREPARE for ballot {}", nodeId, fmt(b));

            List<PromiseRes> promises = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch latch = new CountDownLatch(peers.size());
            ExecutorService fanout = Executors.newFixedThreadPool(peers.size());

            for (Map.Entry<String, PaxosNodeGrpc.PaxosNodeBlockingStub> e : peers.entrySet()) {
                fanout.submit(() -> {
                    try {
                        PromiseRes res = e.getValue()
                                .withDeadline(Deadline.after(rpcDeadlineMs, TimeUnit.MILLISECONDS))
                                .prepare(prepare);
                        if (!isLower(res.getB(), b)) {
                            promises.add(res);
                        }
                    } catch (StatusRuntimeException ex) {
                        log.warn("[{}] Failed to get promise from {}: {}", nodeId, e.getKey(), ex.getStatus());
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(rpcDeadlineMs + 100, TimeUnit.MILLISECONDS);
            fanout.shutdownNow();

            int quorum = (peers.size() / 2) + 1;
            if (promises.size() >= quorum) {
                core.becomeLeader(b);
                log.info("[{}] Won election -> became LEADER for {}", nodeId, fmt(b));
                
                
                PaxosCore.NewViewResult result = core.buildNewViewFromPromises(b, new ArrayList<>(promises));
                core.updateSeqAfterElection(result.maxSeq());
                broadcastNewView(result.req());
                
                recordLeaderActivity();
            } else {
                log.warn("[{}] Election lost (got {}/{} promises); another node may have won.", nodeId, promises.size(), quorum);
            }
        } catch (Exception ex) {
            log.error("[{}] Election error: {}", nodeId, ex.getMessage(), ex);
        } finally {
            electionInProgress.set(false);
        }
    }

    private void broadcastNewView(NewViewReq nv) {
        for (Map.Entry<String, PaxosNodeGrpc.PaxosNodeBlockingStub> e : peers.entrySet()) {
            try {
                e.getValue().withDeadline(Deadline.after(rpcDeadlineMs, TimeUnit.MILLISECONDS)).newView(nv);
            } catch (Exception ex) {
                log.warn("[{}] Failed to send NEW_VIEW to {}: {}", nodeId, e.getKey(), ex.getMessage());
            }
        }
        log.info("[{}] Broadcasted NEW_VIEW with {} accept(s)", nodeId, nv.getRebroadcastBatchCount());
    }

    private static boolean isLower(Ballot a, Ballot b) {
        if (a.getEpoch() != b.getEpoch()) return a.getEpoch() < b.getEpoch();
        return a.getLeaderId().compareTo(b.getLeaderId()) < 0;
    }

    private static String fmt(Ballot b) {
        return "(epoch=" + b.getEpoch() + ", leaderId=" + b.getLeaderId() + ")";
    }

    @Override
    public void destroy() {
        scheduler.shutdownNow();
    }
}