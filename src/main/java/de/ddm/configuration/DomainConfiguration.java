package de.ddm.configuration;

import lombok.Data;

@Data
public class DomainConfiguration {

	private final int inputReaderBatchSize = 10000;

	private final String resultCollectorOutputFileName = "results.txt";

}
