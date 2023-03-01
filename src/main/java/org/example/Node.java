package org.example;

import org.tartarus.snowball.ext.englishStemmer;

public class Node {

    private String word;
    private String posTag;
    private String depLabel;
    private int headIndex;

    public Node(String token) {
        String[] splittedToken = token.split("/");
        String word = splittedToken[0];
        englishStemmer stemmer = new englishStemmer();
        stemmer.setCurrent(word);
        stemmer.stem();
        this.word = stemmer.getCurrent();
        if(splittedToken.length == 4) {
            this.posTag = splittedToken[1].toLowerCase();
            this.depLabel = splittedToken[2];
            this.headIndex = Integer.parseInt(splittedToken[3]) - 1;
        }
        else if(splittedToken.length > 4){
            int len = splittedToken.length;
            this.posTag = splittedToken[len-3].toLowerCase();
            this.depLabel = splittedToken[len-2];
            this.headIndex = Integer.parseInt(splittedToken[len-1]) - 1;
        }
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
