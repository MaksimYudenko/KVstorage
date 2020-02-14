package by.webapp.kvstorage.model;

import lombok.Data;

@Data
public class Document implements Cloneable {

    private String key;
    private String value;

    public Document clone() throws CloneNotSupportedException {
        return (Document) super.clone();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        Document d = (Document) obj;
        return key.equals(d.key) && value.equals(d.value);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 79;
        result = prime * result + key.hashCode();
        result = prime * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("Document [key=").append(key)
                .append(", value=").append(value).append(']').toString();
    }

}