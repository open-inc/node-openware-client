import {
  OPCUAClient,
  AttributeIds,
  ClientSubscription,
  TimestampsToReturn,
  ClientMonitoredItem,
  DataValue,
  NodeClass,
  ClientSession,
  VariantArrayType,
  DataType,
  BrowseDescriptionLike,
  ReferenceDescription
} from "node-opcua";

import { OpenwarePusherInterface } from ".";
import { OpenwareDataItem } from "./OpenwareDataItem";

interface OPCUACrawlerOptions {
  root: BrowseDescriptionLike;
  source: string;
  idPrefix: string;
  namePrefix: string;
  blacklist: string[];
  dry: boolean;
}

const defaults: OPCUACrawlerOptions = {
  root: "ObjectsFolder",
  source: "opcua",
  idPrefix: "opcua~",
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

  private log: string[] = [];
  private ids: string[] = [];

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

      await this.crawl(this.options.root);

      console.log(this.log.join("\n"));
    } catch (err) {
      console.log("Err =", err);
    }
  }

  async crawl(root: BrowseDescriptionLike, depth: number = 0) {
    if (!this.session) {
      throw new Error("Session is unavailable.");
    }

    const browseResult = await this.session.browse(root);

    if (browseResult.references) {
      for (const node of browseResult.references) {
        await this.crawlHandler(node, depth);
      }
    }
  }

  async crawlHandler(node: ReferenceDescription, depth: number) {
    if (this.ids.includes(node.nodeId.toString())) {
      return;
    }

    if (this.options.blacklist.includes(node.nodeId.toString())) {
      return;
    }

    this.ids.push(node.nodeId.toString());

    this.log.push(
      "  ".repeat(depth) +
        node.nodeId.toString() +
        ": " +
        node.displayName.text +
        " (" +
        node.nodeClass.toString() +
        ")"
    );

    await this.crawl(
      { nodeId: node.nodeId, referenceTypeId: "Organizes" },
      depth + 1
    );

    await this.crawl(
      { nodeId: node.nodeId, referenceTypeId: "HasComponent" },
      depth + 1
    );

    if (this.options.dry) {
      return;
    }

    if (node.nodeClass === NodeClass.Variable) {
      this.installSubscription(node);
    }

    if (node.nodeClass === NodeClass.Unspecified) {
      this.installSubscription(node);
    }
  }

  async installSubscription(node: ReferenceDescription) {
    if (!this.subscription) {
      throw new Error("Session is unavailable.");
    }

    const monitoredItem = ClientMonitoredItem.create(
      this.subscription,
      {
        nodeId: node.nodeId,
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
      this.publishValue(node, dataValue);
    });
  }

  async publishValue(node: ReferenceDescription, dataValue: DataValue) {
    const id = node.nodeId.toString();
    const name = node.displayName.text;

    if (!name) {
      console.warn(`Name not found for Node '${id}'.`);
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
            unit: ""
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
