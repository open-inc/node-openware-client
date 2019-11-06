import * as amqp from "amqplib";
import { OpenwareDataItem, OpenwarePusherInterface } from ".";
import { Waiter } from "./helper";

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
  private waiter = new Waiter<[amqp.Connection, amqp.Channel]>();

  constructor(
    connectionSettings: AMQPPusherConnectionSettings,
    publishSettings: AMQPPusherPublishSettings
  ) {
    this.connectionSettings = connectionSettings;
    this.publishSettings = publishSettings;

    this.initConnection();
  }

  private async initConnection() {
    const connection = await amqp.connect(this.connectionSettings);
    const channel = await connection.createChannel();

    const ok = await channel.assertExchange(
      this.publishSettings.exchange,
      this.publishSettings.exchangeType,
      this.publishSettings.exchangeOptions
    );

    this.waiter.set([connection, channel]);
  }

  async publish(item: OpenwareDataItem) {
    const [, channel] = await this.waiter.get();

    channel.publish(
      this.publishSettings.exchange,
      this.publishSettings.routingKey,
      Buffer.from(JSON.stringify(item))
    );
  }

  async close() {
    const [connection] = await this.waiter.get();

    await connection.close();
  }
}
