package de.ddm.singletons;

import de.ddm.configuration.InputConfiguration;

public class InputConfigurationSingleton {

	private static InputConfiguration singleton = new InputConfiguration();

	public static InputConfiguration get() {
		return singleton;
	}

	public static void set(InputConfiguration instance) {
		singleton = instance;
	}
}
