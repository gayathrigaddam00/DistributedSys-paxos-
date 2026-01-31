package com.example.paxos;

import com.example.paxos.v1.*;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import com.google.protobuf.Empty;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;

@Command(name = "test-runner", mixinStandardHelpOptions = true,
        description = "Runs test cases for the Paxos banking application.")
public class TestRunner implements Callable<Integer> {

    private static class TestSet {
        final List<String[]> commands;
        final String liveNodes;
        final int setNumber;

        TestSet(List<String[]> commands, String liveNodes, int setNumber) {
            this.commands = commands;
            this.liveNodes = liveNodes;
            this.setNumber = setNumber;
        }
    }

    @Option(names = {"-f", "--file"}, description = "Path to the test case CSV file.", required = true)
    private String file;
    private List<TestSet> testSets;
    private int currentSetIndex = 0;
    private int currentLeaderPort = 50051;
    public static void main(String[] args) {
        int exitCode = new CommandLine(new TestRunner()).execute(args);
        System.exit(exitCode);
    }
    @Override
    public Integer call() throws IOException, CsvException {
        loadTestCases();
        runCommandLineInterface();
        return 0;
    }
    private void runCommandLineInterface() {
        Scanner scanner = new Scanner(System.in);
        BankingClient queryClient = new BankingClient("localhost", 50051);
        System.out.println("Paxos Test Runner started. Type 'help' for a list of commands.");
        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine().trim();
            if (input.isEmpty()) continue;
            String[] parts = input.split("\\s+");
            String command = parts[0];
            switch (command.toLowerCase()) {
                case "next": processNextSet(); break;
                case "printdb":
                    PrintAllDBResponse allDBs = queryClient.printDB();
                    System.out.println("Databases of all nodes:");
                    for (java.util.Map.Entry<String, PrintDBResponse> entry : allDBs.getAllDbsMap().entrySet()) {
                        System.out.println("Node: " + entry.getKey());
                        for (java.util.Map.Entry<String, Integer> dbEntry : entry.getValue().getDbMap().entrySet()) {
                            System.out.println("  " + dbEntry.getKey() + ": " + dbEntry.getValue());
                        }
                    }
                break;
                case "printlog": handlePrintLog(parts); break;
                case "printstatus": handlePrintStatus(queryClient, parts); break;
                case "printview": System.out.println(queryClient.printView()); break;
                case "help": printHelp(); break;
                case "exit": return;
                default: System.out.println("Unknown command: '" + command + "'. Type 'help' for a list of commands."); break;
            }
        }
    }
    private void handlePrintLog(String[] parts) {
        if (parts.length < 2) {
            System.out.println("Usage: printlog <nodeId> (e.g., printlog n1)");
            return;
        }
        String nodeId = parts[1];
        try {
            int port = 50050 + Integer.parseInt(nodeId.substring(1));
            BankingClient nodeClient = new BankingClient("localhost", port);
            System.out.println("Log for node " + nodeId + ":");
            System.out.println(nodeClient.printLog());
        } catch (Exception e) {
            System.err.println("Could not retrieve log from node " + nodeId + ". Error: " + e.getMessage());
        }
    }

    private void handlePrintStatus(BankingClient client, String[] parts) {
        if (parts.length < 2) {
            System.out.println("Usage: printstatus <sequence_number>");
            return;
        }
        try {
            long seq = Long.parseLong(parts[1]);
            PrintAllStatusResponse allStatuses = client.printStatus(seq);
            System.out.println("Status for sequence number " + seq + " from all nodes:");
            for (java.util.Map.Entry<String, PrintStatusResponse> entry : allStatuses.getAllStatusesMap().entrySet()) {
                System.out.println("Node: " + entry.getKey());
                for (java.util.Map.Entry<String, String> statusEntry : entry.getValue().getStatusMap().entrySet()) {
                    System.out.println("  " + statusEntry.getKey() + ": " + statusEntry.getValue());
                }
            }
        } catch (NumberFormatException e) {
            System.err.println("Invalid sequence number: " + parts[1]);
        }
    }

    private void loadTestCases() throws IOException, CsvException {
        this.testSets = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(file))) {
            List<String[]> allRows = reader.readAll();
            if (allRows.size() <= 1) return;

            List<String[]> currentSetCommands = new ArrayList<>();
            String liveNodesForCurrentSet = "";
            int currentSetNumber = -1;

            for (int i = 1; i < allRows.size(); i++) {
                String[] row = allRows.get(i);
                
                if (row == null || row.length == 0 || (row.length == 1 && (row[0] == null || row[0].trim().isEmpty()))) {
                    continue;
                }
                boolean isNewSet = row.length > 0 && row[0] != null && !row[0].trim().isEmpty();
                if (isNewSet && !currentSetCommands.isEmpty()) {
                    testSets.add(new TestSet(new ArrayList<>(currentSetCommands), liveNodesForCurrentSet, currentSetNumber));
                    currentSetCommands.clear();
                }
                if (isNewSet) {
                    try {
                        currentSetNumber = Integer.parseInt(row[0].trim());
                        liveNodesForCurrentSet = (row.length > 2 && row[2] != null) ? row[2].trim() : "[]";
                    } catch (NumberFormatException e) {
                        continue;
                    }
                }
                currentSetCommands.add(row);
            }
            if (!currentSetCommands.isEmpty() && currentSetNumber != -1) {
                testSets.add(new TestSet(new ArrayList<>(currentSetCommands), liveNodesForCurrentSet, currentSetNumber));
            }
        }
    }
    private void processNextSet() {
        if (currentSetIndex >= testSets.size()) {
            System.out.println("All test sets have been processed. Type 'exit' to quit.");
            return;
        }
        TestSet currentSet = testSets.get(currentSetIndex);
        System.out.println("\n=======================================================");
        System.out.println("Processing test set " + currentSet.setNumber);
        System.out.println("Live Nodes: " + currentSet.liveNodes);
        System.out.println("=======================================================");

        configureLiveNodes(currentSet.liveNodes);
        if (currentSet.setNumber <= 5) {
            try {
                BankingClient n1Client = new BankingClient("localhost", 50051);
                if (!n1Client.isLeader()) {
                    System.out.println("...Starting election.");
                    n1Client.startElection();
                    Thread.sleep(500);
                } else {
                    System.out.println("... already leader. No election needed.");
                }
                this.currentLeaderPort = 50051;
            } catch (Exception e) {
                System.err.println("... leader. Error: " + e.getMessage());
            }
        }

        System.out.println("Node configuration confirmed. Submitting transactions...");
        for (String[] row : currentSet.commands) {
            if (row.length > 1 && row[1] != null && !row[1].trim().isEmpty()) {
                String command = row[1].trim();
                if ("LF".equalsIgnoreCase(command)) {
                    handleLeaderFailure();
                } else {
                    parseAndSubmitTransaction(command);
                }
            }
        }
        currentSetIndex++;
        
        if (currentSetIndex >= testSets.size()) {
            System.out.println("\n=======================================================");
            System.out.println("All " + testSets.size() + " test sets complete.");
            System.out.println("You can now use printdb, printlog, etc. or type 'exit' to quit.");
            System.out.println("=======================================================");
        }
    }

    private void configureLiveNodes(String liveNodesStr) {
        String stripped = liveNodesStr.replaceAll("[\\[\\] ]", "");
        List<String> liveNodeIds = Arrays.asList(stripped.split(","));

        System.out.println("Configuring node statuses...");
        for (int i = 1; i <= 5; i++) {
            String nodeId = "n" + i;
            int port = 50050 + i;
            boolean shouldBeActive = liveNodeIds.contains(nodeId);
            try {
                BankingClient nodeClient = new BankingClient("localhost", port);
                nodeClient.setNodeStatus(shouldBeActive);
            } catch (Exception e) {
                System.err.println("Could not configure status for " + nodeId + ". Is it running?");
            }
        }
        System.out.println("Node configuration complete.");
    }

    private void parseAndSubmitTransaction(String txnCommand) {
        String txnStr = txnCommand.replaceAll("[()]", "");
        String[] txnParts = txnStr.split(", *");

        if (txnParts.length != 3) {
            System.err.println("Skipping malformed transaction format: " + txnCommand);
            return;
        }

        try {
            Txn txn = Txn.newBuilder()
                    .setFrom(txnParts[0].trim())
                    .setTo(txnParts[1].trim())
                    .setAmount(Integer.parseInt(txnParts[2].trim()))
                    .setClientId("client-" + System.currentTimeMillis())
                    .setClientTs(System.currentTimeMillis())
                    .build();

            System.out.println("Submitting transaction: " + txn.getFrom() + " -> " + txn.getTo() + " for " + txn.getAmount());
            int maxRetries = 2;
            for (int i = 0; i < maxRetries; i++) {
            System.out.println("... Attempting to contact node at port " + currentLeaderPort);
            BankingClient client = new BankingClient("localhost", currentLeaderPort);
            
            try {
                ClientReply reply = client.submit(txn);
                if (!"NOT_LEADER".equals(reply.getMessage())) {
                    //System.out.println("... Success! Reply from leader " + reply.getB().getLeaderId() + ": " + reply.getMessage());
                    System.out.println("... Success! Reply from leader ");
                    String leaderId = reply.getB().getLeaderId();
                     if (leaderId != null && !leaderId.isEmpty()) {
                        int leaderNum = Integer.parseInt(leaderId.substring(1));
                        this.currentLeaderPort = 50050 + leaderNum;
                    }
                    return; 
                }
                
                //System.out.println("... Node at " + currentLeaderPort + " is not the leader.");

            } catch (Exception e) {
                
                System.err.println(".might be down. Leader election again");
            }

            this.currentLeaderPort = 50050 + ((currentLeaderPort - 50050) % 5 + 1);
            Thread.sleep(500); 
        }
            System.err.println("FAILED to submit transaction after retries. No leader could be found.");
        } catch (Exception e) {
            System.err.println("An error occurred while submitting transaction: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void handleLeaderFailure() {
        System.out.println("Executing Leader Failure command...");
        String leaderId = null;
        int leaderPort = -1;

        for (int i = 1; i <= 5; i++) {
            try {
                BankingClient nodeClient = new BankingClient("localhost", 50050 + i);
                if (nodeClient.isLeader()) {
                    leaderId = "n" + i;
                    leaderPort = 50050 + i;
                    break;
                }
            } catch (Exception e) { /* Node might be down*/ }
        }

        if (leaderId == null) {
            System.err.println("... Could not determine the current leader. Aborting LF command.");
            return;
        }
        System.out.println("... Current leader is " + leaderId + ". Simulating failure.");

        try {
            BankingClient leaderClient = new BankingClient("localhost", leaderPort);
            leaderClient.setNodeStatus(false);
            System.out.println("... " + leaderId + " has been deactivated.");
        } catch (Exception e) {
            System.err.println("... Failed to deactivate leader " + leaderId + ". Error: " + e.getMessage());
            return;
        }

        int leaderNum = Integer.parseInt(leaderId.substring(1));
        int nextNodeNum = (leaderNum % 5) + 1;
        String nextNodeId = "n" + nextNodeNum;
        int nextNodePort = 50050 + nextNodeNum;
        this.currentLeaderPort = nextNodePort;

        try {
            BankingClient newLeaderClient = new BankingClient("localhost", nextNodePort);
            newLeaderClient.startElection();
            System.out.println("... Election started");
            Thread.sleep(500);
        } catch (Exception e) {
            System.err.println("... Failed to trigger election on Error: " + e.getMessage());
        }
        System.out.println("Leader Failure command complete.");
    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  next         - Process the next set of transactions from the test file.");
        System.out.println("  printdb      - Print the current state of all bank account balances.");
        System.out.println("  printlog     - Print the detailed message log for a specific node (e.g., 'printlog n1').");
        System.out.println("  printstatus  - Print the status for a given sequence number (e.g., 'printstatus 1').");
        System.out.println("  printview    - Print the history of leader views.");
        System.out.println("  help         - Show this help message.");
        System.out.println("  exit         - Exit the test runner.");
    }
}