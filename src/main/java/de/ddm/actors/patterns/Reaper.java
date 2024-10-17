package de.ddm.actors.patterns;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.ReaperSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

public class Reaper extends AbstractBehavior<Reaper.Message> {

	public static <T> void watchWithDefaultReaper(ActorRef<T> actor) {
		ReaperSingleton.get().tell(new WatchMeMessage(actor.unsafeUpcast()));
	}

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class WatchMeMessage implements Message {
		private static final long serialVersionUID = 2674402496050807748L;
		private ActorRef<Void> actor;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "reaper";

	public static Behavior<Message> create() {
		return Behaviors.setup(Reaper::new);
	}

	private Reaper(ActorContext<Message> context) {
		super(context);

		ReaperSingleton.set(this.getContext().getSelf());
	}

	/////////////////
	// Actor State //
	/////////////////

	private final Set<ActorRef<Void>> watchees = new HashSet<>();

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(WatchMeMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(WatchMeMessage message) {
		this.getContext().getLog().info("Watching " + message.getActor().path().name());

		if (this.watchees.add(message.getActor()))
			this.getContext().watch(message.getActor());
		return this;
	}

	private Behavior<Message> handle(Terminated signal) {
		this.watchees.remove(signal.getRef());

		if (!this.watchees.isEmpty())
			return this;

		this.getContext().getLog().info("Every local actor has been reaped. Terminating the actor system...");

		this.getContext().getSystem().terminate();
		return Behaviors.stopped();
	}
}
