import * as amqp from "amqplib";
import { OpenwareDataItem, OpenwarePusherInterface } from ".";

export type AMQPPusherConnectionSettings = string | amqp.Options.Connect;

export interface AMQPPusherPublishSettings {
  routingKey: string;
  exchange: string;
  exchangeType: string;
  exchangeOptions?: amqp.Options.AssertExchange;
}

export class AMQPPusher implements OpenwarePusherInterface {
  private connectionSettings: AMQPPusherConnectionSettings;
  private publishSettings: AMQPPusherPublishSettings;
  private connection: amqp.Connection | null = null;
  private channel: amqp.Channel | null = null;

  constructor(
    connectionSettings: AMQPPusherConnectionSettings,
    publishSettings: AMQPPusherPublishSettings
  ) {
    this.connectionSettings = connectionSettings;
    this.publishSettings = publishSettings;
  }

  private async getChannel(): Promise<amqp.Channel> {
    if (!this.connection || !this.channel) {
      this.connection = await amqp.connect(this.connectionSettings);
      this.channel = await this.connection.createChannel();

      const ok = await this.channel.assertExchange(
        this.publishSettings.exchange,
        this.publishSettings.exchangeType,
        this.publishSettings.exchangeOptions
      );
    }

    return this.channel;
  }

  async publish(item: OpenwareDataItem) {
    const channel = await this.getChannel();

    const ok = await channel.publish(
      this.publishSettings.exchange,
      this.publishSettings.routingKey,
      Buffer.from(JSON.stringify(item))
    );
  }

  async close() {
    if (this.connection) {
      await this.connection.close();
    }

    this.connection = null;
    this.channel = null;
  }
}
