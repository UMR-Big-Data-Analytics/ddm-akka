package de.ddm.singletons;

import akka.actor.typed.ActorRef;
import de.ddm.actors.patterns.Reaper;

public class ReaperSingleton {

	private static ActorRef<Reaper.Message> singleton;

	public static ActorRef<Reaper.Message> get() {
		return singleton;
	}

	public static void set(ActorRef<Reaper.Message> instance) {
		singleton = instance;
	}
}
