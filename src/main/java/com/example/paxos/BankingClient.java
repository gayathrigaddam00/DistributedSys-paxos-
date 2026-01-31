package com.example.paxos;

import com.example.paxos.v1.*;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class BankingClient {
    private final BankGrpc.BankBlockingStub blockingStub;
    private final AdminServiceGrpc.AdminServiceBlockingStub adminStub;

    public BankingClient(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        blockingStub = BankGrpc.newBlockingStub(channel);
        adminStub = AdminServiceGrpc.newBlockingStub(channel);
    }

    public ClientReply submit(com.example.paxos.v1.Txn txn) {
        ClientRequest request = ClientRequest.newBuilder().setTxn(txn).build();
        return blockingStub.submit(request);
    }

    public void setNodeStatus(boolean isActive) {
    String statusString = isActive ? "UP" : "DOWN"; 
    SetStatusRequest request = SetStatusRequest.newBuilder().setStatus(statusString).build();
    adminStub.setNodeStatus(request);
}


    public boolean isLeader() {
        IsLeaderResponse response = adminStub.isLeader(Empty.newBuilder().build());
        return response.getIsLeader();
    }
    
    public void startElection() {
        adminStub.failLeader(Empty.newBuilder().build()); // Re-using failLeader to mean startElection
    }

    public void stopTimers() {
        adminStub.stopTimers(Empty.newBuilder().build());
    }

    public void startTimers() {
        adminStub.startTimers(Empty.newBuilder().build());
    }

   public PrintAllDBResponse printDB() {
    return blockingStub.printDB(Empty.newBuilder().build());
}

    public String printLog() {
        PrintLogResponse response = blockingStub.printLog(PrintLogRequest.newBuilder().build());
        return String.join("\n", response.getLogEntriesList());
    }

    // public PrintStatusResponse printStatus(long seq) {
    //     PrintStatusRequest request = PrintStatusRequest.newBuilder().setSeq(seq).build();
    //     return blockingStub.printStatus(request);
    // }

    public PrintAllStatusResponse printStatus(long seq) {
    PrintStatusRequest request = PrintStatusRequest.newBuilder().setSeq(seq).build();
    return blockingStub.printStatus(request);
}

    public PrintViewResponse printView() {
        return blockingStub.printView(Empty.newBuilder().build());
    }
}