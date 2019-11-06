import { OpenwareDataItem } from ".";

export interface OpenwarePusherInterface {
  publish(item: OpenwareDataItem): Promise<void>;
  close(): Promise<void>;
}
