import * as amqp from "amqplib";

type ConnectionSettings = string | amqp.Options.Connect;

interface PublishSettings {
  routingKey: string;
  exchange: string;
  exchangeType: string;
  exchangeOptions?: amqp.Options.AssertExchange;
}

interface OpenwareDataItem {
  id: string;
  name: string;
  user: string;
  meta?: Record<string, any>;
  valueTypes: {
    name: string;
    unit: string;
    type: string;
  }[];
  values: [
    {
      date: number;
      value: any[];
    }
  ];
}

export class AMQPClient {
  private connectionSettings: ConnectionSettings;
  private publishSettings: PublishSettings;
  private connection: amqp.Connection | null = null;
  private channel: amqp.Channel | null = null;

  constructor(
    connectionSettings: ConnectionSettings,
    publishSettings: PublishSettings
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
