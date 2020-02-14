package by.webapp.kvstorage.model;

import lombok.Data;
import org.hibernate.validator.constraints.Range;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

@Data
@Entity
@Table(name = "collections")
public class Collection implements Cloneable {

    @Id
    @Column(name = "name", updatable = false)
    @NotNull(message = "Name is compulsory")
    @Pattern(regexp = "^[a-zA-Z0-9_]*$", message = "Name has invalid characters")
    private String name;
    @Column(name = "algorithm")
    @NotNull(message = "Algorithm is compulsory")
    @Pattern(regexp = "^[a-zA-Z]{3}$", message = "Algorithm has invalid characters")
    private String algorithm;
    @Column(name = "cache_limit")
    @NotNull(message = "Cache limit is compulsory")
    @Range(min = 1, message = "Cache limit must be positive")
    private Integer cacheLimit;
    @Column(name = "json_schema", columnDefinition = "text", nullable = false)
    private String jsonSchema;

    public Collection clone() throws CloneNotSupportedException {
        return (Collection) super.clone();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        Collection c = (Collection) obj;
        return name.equals(c.name) && algorithm.equals(c.algorithm) &&
                cacheLimit.equals(c.cacheLimit) &&
                jsonSchema.equals(c.jsonSchema);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 79;
        result = prime * result + name.hashCode();
        result = prime * result + algorithm.hashCode();
        result = prime * result + cacheLimit;
        result = prime * result + jsonSchema.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("Collection [name=").append(name)
                .append(", cacheLimit=").append(cacheLimit)
                .append(", algorithm=").append(algorithm)
                .append(", jsonSchema=").append(jsonSchema).append(']').toString();
    }

}