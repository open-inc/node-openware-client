export class Waiter<T> {
  value: T | null = null;
  waiting: ((value: T) => void)[] = [];

  async get(): Promise<T> {
    if (this.value) {
      return this.value;
    }

    return new Promise(resolve => {
      this.waiting.push(resolve);
    });
  }

  set(value: T) {
    this.value = value;

    this.waiting.forEach(resolve => resolve(value));
  }
}
