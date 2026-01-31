package com.example.paxos.transport;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.election.ElectionDriver;
import com.example.paxos.v1.*;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.context.annotation.Lazy;
import com.google.protobuf.Empty;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@GrpcService
public class BankGrpcService extends BankGrpc.BankImplBase {

    private final PaxosCore core;
    private final String nodeId;
    private final Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peers;
    private final ElectionDriver electionDriver;
    private final long clientWaitTimeoutMillis = 5000;
    private final long peerTimeoutMillis = 500;

    public BankGrpcService(PaxosCore core, String nodeId, Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peers, @Lazy ElectionDriver electionDriver) {
        this.core = core;
        this.nodeId = nodeId;
        this.peers = peers;
        this.electionDriver = electionDriver;
    }

    @Override
    public void submit(ClientRequest req, StreamObserver<ClientReply> responseObserver) {
        try {
            if (!core.hasLeader()) {
                electionDriver.startElection();
            }
            Object action = core.onClientSubmit(req.getTxn());
            if (action instanceof PaxosCore.ReplyAction ra) {
                responseObserver.onNext(ra.reply);
                responseObserver.onCompleted();
                return;
            }
            if (action instanceof PaxosCore.Broadcast b) {
                for (Object m : b.messages) {
                    if (m instanceof AcceptReq a) {
                        for (Map.Entry<String, PaxosNodeGrpc.PaxosNodeBlockingStub> e : peers.entrySet()) {
                            if (!e.getKey().equals(nodeId)) {
                                try { 
                                PaxosNodeGrpc.PaxosNodeBlockingStub stub = e.getValue()
                                .withDeadlineAfter(peerTimeoutMillis, TimeUnit.MILLISECONDS);
                        
                        stub.accept(a);  } catch (Exception ignored) {}
                            }
                        }
                    }
                }
                String clientId = req.getTxn().getClientId();
                long clientTs = req.getTxn().getClientTs();
                Instant deadline = Instant.now().plusMillis(clientWaitTimeoutMillis);
                ClientReply cached = core.getCachedReply(clientId);
                while (cached == null || cached.getClientTs() < clientTs) {
                    if (Instant.now().isAfter(deadline)) {
                        ClientReply timeoutReply = ClientReply.newBuilder()
                                .setB(core.getCurrentLeader() != null ? core.getCurrentLeader() : Ballot.getDefaultInstance())
                                .setClientId(clientId).setClientTs(clientTs).setSuccess(false).setMessage("TIMEOUT_WAITING_FOR_COMMIT").build();
                        responseObserver.onNext(timeoutReply);
                        responseObserver.onCompleted();
                        return;
                    }
                    try { TimeUnit.MILLISECONDS.sleep(50); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                    cached = core.getCachedReply(clientId);
                }
                responseObserver.onNext(cached);
                responseObserver.onCompleted();
                return;
            }
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription("Unknown PaxosCore result").asRuntimeException());
        } catch (Exception ex) {
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
        }
    }

   @Override
public void printDB(com.google.protobuf.Empty request, 
                    io.grpc.stub.StreamObserver<com.example.paxos.v1.PrintAllDBResponse> responseObserver) {
    com.example.paxos.v1.PrintAllDBResponse.Builder responseBuilder = com.example.paxos.v1.PrintAllDBResponse.newBuilder();

    // Get local DB
    com.example.paxos.v1.PrintDBResponse localDb = com.example.paxos.v1.PrintDBResponse.newBuilder().putAllDb(core.printDB()).build();
    responseBuilder.putAllDbs(nodeId, localDb);

    // Get peer DBs
    for (java.util.Map.Entry<String, com.example.paxos.v1.PaxosNodeGrpc.PaxosNodeBlockingStub> entry : peers.entrySet()) {
        if (!entry.getKey().equals(nodeId)) {
            try {
                com.example.paxos.v1.PrintDBResponse peerDb = entry.getValue().getDB(com.google.protobuf.Empty.newBuilder().build());
                responseBuilder.putAllDbs(entry.getKey(), peerDb);
            } catch (Exception e) {
                // You can add logging here to see which nodes failed to respond.
                System.err.println("Failed to get DB from node: " + entry.getKey());
            }
        }
    }

    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
}

    @Override
    public void printLog(PrintLogRequest request, StreamObserver<PrintLogResponse> responseObserver) {
        PrintLogResponse.Builder response = PrintLogResponse.newBuilder();
        response.addAllLogEntries(core.printLog());

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    // @Override
    // public void printStatus(PrintStatusRequest request, StreamObserver<PrintStatusResponse> responseObserver) {
    //     PrintStatusResponse.Builder response = PrintStatusResponse.newBuilder();
    //     Map<String, PaxosCore.Status> statusMap = core.printStatus(request.getSeq());
    //     for (Map.Entry<String, PaxosCore.Status> entry : statusMap.entrySet()) {
    //         response.putStatus(entry.getKey(), entry.getValue().name());
    //     }
    //     responseObserver.onNext(response.build());
    //     responseObserver.onCompleted();
    // }

    @Override
public void printStatus(com.example.paxos.v1.PrintStatusRequest request, 
                        io.grpc.stub.StreamObserver<com.example.paxos.v1.PrintAllStatusResponse> responseObserver) {
    com.example.paxos.v1.PrintAllStatusResponse.Builder responseBuilder = com.example.paxos.v1.PrintAllStatusResponse.newBuilder();
    com.example.paxos.v1.PrintStatusResponse.Builder localStatusBuilder = com.example.paxos.v1.PrintStatusResponse.newBuilder();
    java.util.Map<String, com.example.paxos.core.PaxosCore.Status> localStatusMap = core.printStatus(request.getSeq());
    for (java.util.Map.Entry<String, com.example.paxos.core.PaxosCore.Status> entry : localStatusMap.entrySet()) {
        localStatusBuilder.putStatus(entry.getKey(), entry.getValue().name());
    }
    responseBuilder.putAllStatuses(nodeId, localStatusBuilder.build());

    // Get peer statuses
    for (java.util.Map.Entry<String, com.example.paxos.v1.PaxosNodeGrpc.PaxosNodeBlockingStub> entry : peers.entrySet()) {
        if (!entry.getKey().equals(nodeId)) {
            try {
                com.example.paxos.v1.PrintStatusResponse peerStatus = entry.getValue().getStatus(request);
                responseBuilder.putAllStatuses(entry.getKey(), peerStatus);
            } catch (Exception e) {
                System.err.println("Failed to get status from node: " + entry.getKey());
            }
        }
    }

    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
}
    @Override
    public void printView(Empty request, StreamObserver<PrintViewResponse> responseObserver) {
        PrintViewResponse.Builder response = PrintViewResponse.newBuilder();
        response.addAllViews(core.printView());
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }
}