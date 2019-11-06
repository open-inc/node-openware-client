export interface OpenwareDataItemValueType {
  name: string;
  unit: string;
  type: string;
}

export interface OpenwareDataItem {
  id: string;
  name: string;
  user: string;
  meta: Record<string, any>;
  valueTypes: OpenwareDataItemValueType[];
  values: [
    {
      date: number;
      value: any[];
    }
  ];
}
