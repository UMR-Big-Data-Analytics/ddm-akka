package de.ddm;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Objects;

public class LargeMessageProxyTest {

	@ClassRule
	public static final TestKitJunitResource testKit = new TestKitJunitResource(SystemConfigurationSingleton.get().toAkkaTestConfig());

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class MyLargeMessage implements LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = -6299751781749878256L;

		private String content;
		private ActorRef<LargeMessageProxy.Message> largeMessageProxy;

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || this.getClass() != o.getClass())
				return false;
			MyLargeMessage other = (MyLargeMessage) o;
			return Objects.equals(this.content, other.content);
		}

		@Override
		public int hashCode() {
			return Objects.hash(content);
		}
	}

	@Test
	public void testLargeMessageSending() {
		LargeMessageProxy.MAX_MESSAGE_SIZE = 2;

		TestProbe<LargeMessageProxy.LargeMessage> probe = testKit.createTestProbe();

		ActorRef<LargeMessageProxy.Message> senderLargeMessageProxy = testKit.spawn(LargeMessageProxy.create(probe.getRef(), true), "sender_" + LargeMessageProxy.DEFAULT_NAME);
		ActorRef<LargeMessageProxy.Message> receiverLargeMessageProxy = testKit.spawn(LargeMessageProxy.create(probe.getRef(), true), "receiver_" + LargeMessageProxy.DEFAULT_NAME);

		LargeMessageProxy.LargeMessage message = new MyLargeMessage("Hello World! This is some large message with a large string.", senderLargeMessageProxy);

		senderLargeMessageProxy.tell(new LargeMessageProxy.SendMessage(message, receiverLargeMessageProxy));

		probe.expectMessage(message);
		probe.expectNoMessage();
	}

	@Test
	public void testProtocol() {
		TestProbe<LargeMessageProxy.LargeMessage> probe = testKit.createTestProbe();

		ActorRef<LargeMessageProxy.Message> largeMessageProxy = testKit.spawn(LargeMessageProxy.create(probe.getRef(), true), LargeMessageProxy.DEFAULT_NAME);

		LargeMessageProxy.LargeMessage message = new MyLargeMessage();

		largeMessageProxy.tell(new LargeMessageProxy.SendMessage(message, largeMessageProxy));

		probe.expectMessage(message);
		probe.expectNoMessage();
	}
}