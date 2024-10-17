package de.ddm.singletons;

import de.ddm.configuration.DomainConfiguration;

public class DomainConfigurationSingleton {

	private static DomainConfiguration singleton = new DomainConfiguration();

	public static DomainConfiguration get() {
		return singleton;
	}

	public static void set(DomainConfiguration instance) {
		singleton = instance;
	}
}
