package org.hibernate.ogm.examples.gettingstarted.domain;



import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Emmanuel Bernard
 */
@Entity
public class Cloud {
	private String id;
	private String type;
	private double length;
	private Set<SnowFlake> producedSnowFlakes = new HashSet<SnowFlake>();

	@Id
	@GeneratedValue(generator = "uuid")
	@GenericGenerator(name = "uuid", strategy = "uuid2")
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public double getLength() {
		return length;
	}

	public void setLength(double length) {
		this.length = length;
	}

	@OneToMany
	@JoinTable
	public Set<SnowFlake> getProducedSnowFlakes() {
		return producedSnowFlakes;
	}

	public void setProducedSnowFlakes(Set<SnowFlake> producedSnowFlakes) {
		this.producedSnowFlakes = producedSnowFlakes;
	}
}
