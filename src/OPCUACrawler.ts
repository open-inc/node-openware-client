import {
  OPCUAClient,
  NodeCrawler,
  AttributeIds,
  ClientSubscription,
  TimestampsToReturn,
  ClientMonitoredItem,
  DataValue,
  NodeClass,
  QualifiedName,
  NodeId,
  ClientSession,
  VariantArrayType,
  DataType
} from "node-opcua";

let i = 0;

import { OpenwarePusherInterface } from ".";
import { OpenwareDataItem } from "./OpenwareDataItem";

interface OPCUACrawlerOptions {
  root: string;
  source: string;
  idPrefix: string;
  namePrefix: string;
  blacklist: string[];
  dry: boolean;
}

const defaults: OPCUACrawlerOptions = {
  root: "ObjectsFolder",
  source: "opcua",
  idPrefix: "opcua/",
  namePrefix: "OPC UA: ",
  blacklist: [],
  dry: false
};

export class OPCUACrawler {
  private pusher: OpenwarePusherInterface;
  private options: OPCUACrawlerOptions;

  private client: OPCUAClient | null = null;
  private session: ClientSession | null = null;
  private subscription: ClientSubscription | null = null;

  private nameCache = new Map<string, string>();

  constructor(
    pusher: OpenwarePusherInterface,
    opcuaEndpoint: string,
    options: Partial<OPCUACrawlerOptions>
  ) {
    this.pusher = pusher;
    this.options = Object.assign({}, defaults, options);

    this.init(opcuaEndpoint);
  }

  async init(opcuaEndpoint: string) {
    try {
      this.client = OPCUAClient.create({});

      this.client.on("backoff", () => console.log("backoff"));

      await this.client.connect(opcuaEndpoint);

      this.session = await this.client.createSession();

      this.subscription = ClientSubscription.create(this.session, {
        requestedPublishingInterval: 1000,
        requestedLifetimeCount: 100,
        requestedMaxKeepAliveCount: 10,
        maxNotificationsPerPublish: 100,
        publishingEnabled: true,
        priority: 10
      });

      this.subscription
        .on("started", () => {
          console.log("subscription started");
        })
        .on("keepalive", () => {
          console.log("keepalive");
        })
        .on("terminated", () => {
          console.log("terminated");
        });

      this.crawl();
    } catch (err) {
      console.log("Err =", err);
    }
  }

  async crawl() {
    if (!this.session) {
      throw new Error("Session is unavailable.");
    }

    const crawler = new NodeCrawler(this.session);

    crawler.on("browsed", (node: any) => {
      const id: NodeId = node.nodeId;
      const name: QualifiedName = node.browseName;
      const nodeClass: NodeClass = node.nodeClass;

      if (this.options.dry) {
        console.log(id.toString(), name.toString());
        return;
      }

      if (this.options.blacklist.includes(id.toString())) {
        return;
      }

      this.nameCache.set(id.toString(), name.toString());

      if (nodeClass === NodeClass.Variable) {
        this.handleVariable(id);
      }
    });

    crawler.read(this.options.root);
  }

  async handleVariable(id: NodeId) {
    this.installSubscription(id);
  }

  async installSubscription(nodeId: NodeId) {
    if (!this.subscription) {
      throw new Error("Session is unavailable.");
    }

    const monitoredItem = ClientMonitoredItem.create(
      this.subscription,
      {
        nodeId,
        attributeId: AttributeIds.Value
      },
      {
        samplingInterval: 100,
        discardOldest: true,
        queueSize: 10
      },
      TimestampsToReturn.Both
    );

    monitoredItem.on("changed", (dataValue: DataValue) => {
      this.publishValue(nodeId, dataValue);
    });
  }

  async publishValue(nodeId: NodeId, dataValue: DataValue) {
    const id = nodeId.toString();
    const name = this.nameCache.get(id);

    if (!name) {
      throw new Error(`Name not found for Node '${id}'.`);
    }

    const item: OpenwareDataItem = {
      id: this.options.idPrefix + id,
      name: this.options.namePrefix + name,
      user: this.options.source,
      meta: {
        opcuaDataValue: dataValue.value.toJSON()
      },
      valueTypes: [this.mapDataValueToValueType(dataValue)],
      values: [
        {
          date: dataValue.serverTimestamp
            ? dataValue.serverTimestamp.valueOf()
            : 0,
          value: [this.mapDataValueToValue(dataValue)]
        }
      ]
    };

    // console.log(++i, id, name);

    this.pusher.publish(item).then(
      ok => {},
      error => {
        console.error(error);
      }
    );
  }

  private mapDataValueToValue(dataValue: DataValue): any {
    switch (dataValue.value.dataType) {
      default:
        return dataValue.value.value;
    }
  }

  private mapDataValueToValueType(
    dataValue: DataValue
  ): {
    name: string;
    unit: string;
    type: string;
  } {
    const name = "Wert";

    if (dataValue.value.arrayType === VariantArrayType.Scalar) {
      switch (dataValue.value.dataType) {
        case DataType.String:
          return {
            name,
            type: "String",
            unit: ""
          };

        case DataType.Int16:
        case DataType.UInt16:
        case DataType.Int32:
        case DataType.UInt32:
        case DataType.Int64:
        case DataType.UInt64:
        case DataType.Float:
        case DataType.Double:
          return {
            name,
            type: "Number",
            unit: ""
          };

        case DataType.DateTime:
          return {
            name,
            type: "Number",
            unit: "Date"
          };

        case DataType.Boolean:
        case DataType.Byte:
          return {
            name,
            type: "Boolean",
            unit: "Date"
          };

        // case DataType.Null:
        // case DataType.SByte:
        // case DataType.Guid:
        // case DataType.ByteString:
        // case DataType.XmlElement:
        // case DataType.NodeId:
        // case DataType.ExpandedNodeId:
        // case DataType.StatusCode:
        // case DataType.QualifiedName:
        // case DataType.LocalizedText:
        // case DataType.ExtensionObject:
        // case DataType.DataValue:
        // case DataType.Variant:
        // case DataType.DiagnosticInfo:
      }
    }

    return {
      name,
      type: "Object",
      unit: ""
    };
  }
}
