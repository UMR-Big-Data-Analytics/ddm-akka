package de.ddm.configuration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;

import java.nio.charset.Charset;

@Parameters(commandDescription = "Start a master ActorSystem.")
public class CommandMaster extends Command {

	@Override
	int getDefaultPort() {
		return SystemConfiguration.DEFAULT_MASTER_PORT;
	}

	@Parameter(names = {"-sp", "--startPaused"}, description = "Wait for some console input to start the discovery; useful, if we want to wait manually until all ActorSystems in the cluster are started (e.g. to avoid work stealing effects in performance evaluations)", required = false, arity = 1)
	boolean startPaused = SystemConfigurationSingleton.get().isStartPaused();

	@Parameter(names = {"-hm", "--hardMode"}, description = "Solve the hard version of the task", required = false, arity = 1)
	boolean hardMode = SystemConfigurationSingleton.get().isHardMode();

	@Parameter(names = {"-ip", "--inputPath"}, description = "Input path for the input data; all files in this folder are considered", required = false, arity = 1)
	String inputPath = InputConfigurationSingleton.get().getInputPath();

	@Parameter(names = {"-fh", "--fileHasHeader"}, description = "File has header as defined by the input data", required = false, arity = 1)
	boolean fileHasHeader = InputConfigurationSingleton.get().isFileHasHeader();

	@Parameter(names = {"-cs", "--charset"}, description = "Charset as defined by the input data", required = false)
	Charset charset = InputConfigurationSingleton.get().getCharset();

	@Parameter(names = {"-vs", "--valueSeparator"}, description = "Value separator as defined by the input data", required = false)
	char attributeSeparator = InputConfigurationSingleton.get().getValueSeparator();

	@Parameter(names = {"-vq", "--valueQuote"}, description = "Value quote as defined by the input data", required = false)
	char attributeQuote = InputConfigurationSingleton.get().getValueQuote();

	@Parameter(names = {"-ve", "--valueEscape"}, description = "Value escape as defined by the input data", required = false)
	char attributeEscape = InputConfigurationSingleton.get().getValueEscape();

	@Parameter(names = {"-vsq", "--valueStrictQuotes"}, description = "Value strict quotes as defined by the input data", required = false, arity = 1)
	boolean attributeStrictQuotes = InputConfigurationSingleton.get().isValueStrictQuotes();

	@Parameter(names = {"-viw", "--valueIgnoreLeadingWhitespace"}, description = "Ignore i.e. delete all whitespaces preceding any read value", required = false, arity = 1)
	boolean attributeIgnoreLeadingWhitespace = InputConfigurationSingleton.get().isValueIgnoreLeadingWhitespace();
}
