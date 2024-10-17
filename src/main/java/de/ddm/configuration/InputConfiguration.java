package de.ddm.configuration;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvValidationException;
import lombok.Data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

@Data
public class InputConfiguration {

	private String inputPath = "data" + File.separator + "TPCH";
	private boolean fileHasHeader = true;
	private Charset charset = StandardCharsets.UTF_8;
	private char valueSeparator = ';';
	private char valueQuote = '"';
	private char valueEscape = '\\';
	private boolean valueStrictQuotes = false;
	private boolean valueIgnoreLeadingWhitespace = false;

	public void update(CommandMaster commandMaster) {
		this.inputPath = commandMaster.inputPath;
		this.fileHasHeader = commandMaster.fileHasHeader;
		this.charset = commandMaster.charset;
		this.valueSeparator = commandMaster.attributeSeparator;
		this.valueQuote = commandMaster.attributeQuote;
		this.valueEscape = commandMaster.attributeEscape;
		this.valueStrictQuotes = commandMaster.attributeStrictQuotes;
		this.valueIgnoreLeadingWhitespace = commandMaster.attributeIgnoreLeadingWhitespace;
	}

	public File[] getInputFiles() {
		File inputFolder = new File(this.inputPath);
		if (!inputFolder.exists())
			throw new RuntimeException("Input folder " + this.inputPath + " does not exists!");

		File[] inputFiles = inputFolder.listFiles();
		if ((inputFiles == null) || (inputFiles.length == 0))
			throw new RuntimeException("Input folder " + this.inputPath + " is empty!");

		return inputFiles;
	}

	public CSVReader createCSVReader(File inputFile) throws IOException {
		CSVParser parser = new CSVParserBuilder()
				.withSeparator(this.valueSeparator)
				.withQuoteChar(this.valueQuote)
				.withEscapeChar(this.valueEscape)
				.withStrictQuotes(this.valueStrictQuotes)
				.withIgnoreLeadingWhiteSpace(this.valueIgnoreLeadingWhitespace)
				.withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
				.build();

		BufferedReader buffer = Files.newBufferedReader(inputFile.toPath(), this.charset);
		return new CSVReaderBuilder(buffer).withCSVParser(parser).build();
	}

	public String[] getHeader(File inputFile) throws IOException, CsvValidationException {
		CSVReader reader = this.createCSVReader(inputFile);

		String[] line = reader.readNext();
		reader.close();

		if (!this.fileHasHeader)
			for (int i = 0; i < line.length; i++)
				line[i] = "Attr_" + (i + 1);
		return line;
	}
}
