package org.example;

import org.apache.commons.lang3.tuple.MutablePair;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.BFSShortestPath;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.apache.hadoop.io.Text;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DependencyGraph {
    private SimpleDirectedGraph<Node, DefaultEdge> graph;
    private int count;
    private List<MutablePair<String,NounPair>> patterns;

    public DependencyGraph(String line) {
        String[] splittedLine = line.split("\t");
        List<Node> nodes = this.parseNodes(splittedLine[1]);
        this.count = Integer.parseInt(splittedLine[2]);
        this.graph = new SimpleDirectedGraph<>(DefaultEdge.class);
        buildGraph(nodes);
        this.patterns = new ArrayList<>();
        findPatterns(nodes);
    }


    private void findPatterns(List<Node> nodes) {
        List<String> nounsList = Arrays.asList("nn", "nns", "nnp", "nnps");
        BFSShortestPath<Node, DefaultEdge> bfs = new BFSShortestPath<Node, DefaultEdge>(this.graph);
        for (Node first : nodes) {
            if (!nounsList.contains(first.getPosTag()))
                continue;
            for (Node last : nodes) {
                if (first.equals(last) || !nounsList.contains(last.getPosTag()))
                    continue;
                GraphPath<Node, DefaultEdge> path = bfs.getPath(first, last);
                if (path != null && path.getVertexList().size() > 1) {
                    patterns.add(generatePattern(path.getVertexList(), this.count));
                }
            }
        }
    }

    private MutablePair<String, NounPair> generatePattern(List<Node> vertexList, int total_count2) {
        Node first = vertexList.get(0);
        Node last = vertexList.get(vertexList.size()-1);

        NounPair pair = new NounPair(new Text(first.getWord()) , new Text(last.getWord()), count);
        String pattern = "";
        for(int i=0; i < vertexList.size() - 1; i++){
            pattern += vertexList.get(i).getPosTag() + ":" + vertexList.get(i).getDepLabel() + ":";
        }
        pattern += vertexList.get(vertexList.size() - 1).getPosTag();

        return new MutablePair<String,NounPair>(pattern, pair);
    }


    private void buildGraph(List<Node> nodes) {
        for (Node node : nodes) {
            graph.addVertex(node);
        }
        for (Node node : nodes) {
            if (node.getHeadIndex() > 0)
                graph.addEdge(nodes.get(node.getHeadIndex()), node);
        }
    }

    private List<Node> parseNodes(String s) {
        List<Node> nodes = new ArrayList<Node>();
        String[] splittedLine = s.split(" ");
        for (String node : splittedLine) {
                nodes.add(new Node(node));
        }
        return nodes;
    }

    public List<MutablePair<String, NounPair>> getPatterns() {
        return patterns;
    }

}
