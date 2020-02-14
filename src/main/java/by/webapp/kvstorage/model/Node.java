package by.webapp.kvstorage.model;

import lombok.Getter;

@Getter
public class Node {

    private String name;
    private String url;
    private Integer group;

    public Node(String name, String url, Integer group) {
        this.name = name;
        this.url = url;
        this.group = group;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        Node node = (Node) obj;
        return name.equals(node.name) && url.equals(node.url) && group.equals(node.group);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 79;
        result = prime * result + name.hashCode();
        result = prime * result + url.hashCode();
        result = prime * result + group.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("Node [name=").append(name)
                .append(", url=").append(url)
                .append(", group=").append(group).append(']').toString();
    }

}