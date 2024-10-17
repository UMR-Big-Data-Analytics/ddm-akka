package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;

@Getter
@AllArgsConstructor
public class InclusionDependency {

	private final File dependentFile;
	private final String[] dependentAttributes;

	private final File referencedFile;
	private final String[] referencedAttributes;

	@Override
	public String toString() {
		return this.fileNameOf(this.dependentFile) + " -> " + this.fileNameOf(this.referencedFile) + ": " +
				Arrays.toString(this.dependentAttributes) + " c " + Arrays.toString(this.referencedAttributes);
	}

	private String fileNameOf(File file) {
		return file.getName().split("\\.")[0];
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		InclusionDependency other = (InclusionDependency) o;
		return Objects.equals(this.dependentFile, other.dependentFile) &&
				Arrays.equals(this.dependentAttributes, other.dependentAttributes) &&
				Objects.equals(this.referencedFile, other.referencedFile) &&
				Arrays.equals(this.referencedAttributes, other.referencedAttributes);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(dependentFile, referencedFile);
		result = 31 * result + Arrays.hashCode(dependentAttributes);
		result = 31 * result + Arrays.hashCode(referencedAttributes);
		return result;
	}
}
