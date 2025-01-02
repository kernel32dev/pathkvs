const net = require('net');
const { Buffer } = require('buffer');

const message = {
    LEN: 1,
    GET: 2,
    SET: 3,
    START_TRANSACTION: 4,
    COMMIT: 5,
    ROLLBACK: 6,
    LIMIT_EXCEEDED: 254,
    CONFLICT: 255
};

class Connection {
    constructor(socket) {
        this.socket = socket;
        this.dataBuffer = Buffer.alloc(0);
        this.socket.on('data', (data) => this.onData(data));
    }

    onData(data) {
        this.dataBuffer = Buffer.concat([this.dataBuffer, data]);
    }

    async writeBuffer(buffer) {
        return new Promise((resolve, reject) => {
            this.socket.write(buffer, (err) => {
                if (err) return reject(err);
                resolve();
            });
        });
    }

    async readByte() {
        await this.ensureData(1);
        const byte = this.dataBuffer.readUInt8(0);
        this.dataBuffer = this.dataBuffer.subarray(1);
        return byte;
    }

    async readUInt32() {
        await this.ensureData(4);
        const int = this.dataBuffer.readUInt32BE(0);
        this.dataBuffer = this.dataBuffer.subarray(4);
        return int;
    }

    async readBytes(length) {
        await this.ensureData(length);
        const bytes = this.dataBuffer.slice(0, length);
        this.dataBuffer = this.dataBuffer.slice(length);
        return bytes;
    }

    async ensureData(length) {
        while (this.dataBuffer.length < length) {
            await new Promise(resolve => this.socket.once('data', resolve));
        }
    }

    async len(key) {
        const keyBuffer = Buffer.from(key);
        if (keyBuffer.length > 0xFFFFFFFF) throw new Error('Key length exceeds maximum limit');

        await this.writeBuffer(Buffer.concat([
            Buffer.from([message.LEN]),
            be(keyBuffer.length),
            keyBuffer
        ]));

        if (await this.readByte() !== message.LEN) throw new Error('Protocol error');
        return await this.readUInt32();
    }

    async read(key) {
        return this.readLimited(key, 0xFFFFFFFF);
    }

    async readLimited(key, maxLen) {
        const result = await this.readLimitedOpt(key, maxLen);
        if (result === null) throw new Error('Limit exceeded');
        return result;
    }

    async readLimitedOpt(key, maxLen) {
        const keyBuffer = Buffer.from(key);
        if (keyBuffer.length > 0xFFFFFFFF) throw new Error('Key length exceeds maximum limit');

        await this.writeBuffer(Buffer.concat([
            Buffer.from([message.GET]),
            be(keyBuffer.length),
            keyBuffer,
            be(maxLen)
        ]));

        const response = await this.readByte();
        if (response === message.GET) {
            const recvLen = await this.readUInt32();
            if (recvLen > maxLen) throw new Error('Protocol error');
            return await this.readBytes(recvLen);
        } else if (response === message.LIMIT_EXCEEDED) {
            return null;
        } else {
            throw new Error('Protocol error');
        }
    }

    async write(key, value) {
        const keyBuffer = Buffer.from(key);
        const valueBuffer = Buffer.from(value);

        if (keyBuffer.length > 0xFFFFFFFF || valueBuffer.length > 0xFFFFFFFF) {
            throw new Error('Key or value length exceeds maximum limit');
        }

        await this.writeBuffer(Buffer.concat([
            Buffer.from([message.SET]),
            be(keyBuffer.length),
            keyBuffer,
            be(valueBuffer.length),
            valueBuffer
        ]));

        if (await this.readByte() !== message.SET) throw new Error('Protocol error');
    }

    async startTransaction() {
        await this.writeBuffer(Buffer.from([message.START_TRANSACTION]));
        if (await this.readByte() !== message.START_TRANSACTION) throw new Error('Protocol error');
    }

    async commit() {
        await this.writeBuffer(Buffer.from([message.COMMIT]));
        const response = await this.readByte();
        if (response === message.COMMIT) {
            return;
        } else if (response === message.CONFLICT) {
            throw new Error('Transaction conflict');
        } else {
            throw new Error('Protocol error');
        }
    }

    async rollback() {
        await this.writeBuffer(Buffer.from([message.ROLLBACK]));
        if (await this.readByte() !== message.ROLLBACK) throw new Error('Protocol error');
    }
}

function be(num) {
    let buffer = Buffer.alloc(4);
    buffer.writeInt32LE(num);
    return buffer;
}

const client = new net.Socket();
client.connect(6314, '127.0.0.1', async () => {
    console.log('Connected');
    // Usage example
    const conn = new Connection(client);
    const result = await conn.write('key', 'value');
    const length = await conn.len('key');
});
