package org.example;

public class Node {

    private String word;
    private String posTag;
    private String depLabel;
    private int headIndex;

    public Node(String token) {

        String[] splittedToken = token.split("/");
        this.posTag = splittedToken[1].toLowerCase();
        this.depLabel = splittedToken[2];
        this.headIndex = Integer.parseInt(splittedToken[3]) - 1;

        Stemmer stemmer = new Stemmer();
        stemmer.add(to_stem.toCharArray(), to_stem.length());
        stemmer.stem();
        this.word = stemmer.toString();

    }

    public String getWord() {
        return word;
    }

    public String getPosTag() {
        return posTag;
    }

    public String getDepLabel() {
        return depLabel;
    }

    public int getHeadIndex() {
        return headIndex;
    }

}
