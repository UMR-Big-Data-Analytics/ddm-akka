package de.ddm.configuration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import de.ddm.singletons.SystemConfigurationSingleton;

@Parameters(commandDescription = "Start a worker ActorSystem.")
public class CommandWorker extends Command {

	@Override
	int getDefaultPort() {
		return SystemConfiguration.DEFAULT_WORKER_PORT;
	}

	@Parameter(names = {"-mh", "--masterhost"}, description = "The host name or IP of the master", required = false)
	String masterhost = SystemConfigurationSingleton.get().getHost();

	@Parameter(names = {"-mp", "--masterport"}, description = "The port of the master", required = false)
	int masterport = SystemConfiguration.DEFAULT_MASTER_PORT;

}
