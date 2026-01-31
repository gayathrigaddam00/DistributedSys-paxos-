package com.example.paxos.transport;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.election.ElectionDriver;
import com.example.paxos.v1.*;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.context.annotation.Lazy;

/**
 * gRPC service implementation for AdminService.
 */
@GrpcService
public class AdminServiceGrpcService extends AdminServiceGrpc.AdminServiceImplBase {

    private final PaxosCore core;
    private final String nodeId;
    private final ElectionDriver electionDriver;

    public AdminServiceGrpcService(PaxosCore core, String nodeId, @Lazy ElectionDriver electionDriver) {
        this.core = core;
        this.nodeId = nodeId;
        this.electionDriver = electionDriver;
    }

     @Override
    public void startTimers(Empty request, StreamObserver<Empty> responseObserver) {
        electionDriver.startTimers();
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void stopTimers(Empty request, StreamObserver<Empty> responseObserver) {
        electionDriver.stopTimers();
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void setNodeStatus(SetStatusRequest request, StreamObserver<Empty> responseObserver) {
        try {
            String status = request.getStatus();
            boolean isActive = "UP".equalsIgnoreCase(status);
            core.setActive(isActive);
            System.out.println("[" + nodeId + "] Node status changed to: " + status + " (active=" + isActive + ")");  
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            System.err.println("[" + nodeId + "] Error setting node status: " + ex.getMessage());
            responseObserver.onError(io.grpc.Status.INTERNAL
                .withDescription("Failed to set node status: " + ex.getMessage())
                .withCause(ex)
                .asRuntimeException());
        }
    }

    @Override
    public void isLeader(Empty request, StreamObserver<IsLeaderResponse> responseObserver) {
        boolean isLeader = core.isLeader();
        IsLeaderResponse response = IsLeaderResponse.newBuilder()
            .setIsLeader(isLeader)
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void failLeader(Empty request, StreamObserver<Empty> responseObserver) {
        electionDriver.startElection();
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

}

