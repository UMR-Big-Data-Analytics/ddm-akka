package de.ddm.singletons;

import de.ddm.configuration.OutputConfiguration;

public class OutputConfigurationSingleton {

	private static OutputConfiguration singleton = new OutputConfiguration();

	public static OutputConfiguration get() {
		return singleton;
	}

	public static void set(OutputConfiguration instance) {
		singleton = instance;
	}
}
