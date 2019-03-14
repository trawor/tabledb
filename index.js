const TableStore = require('tablestore');

class _Classname_ {
  static classname() {
    const name = this.toString().split('(' || /s+/)[0].split(' ' || /s+/)[1];
    return name;
  }

  classname() {
    return this.constructor.name;
  }
}

class TableDB extends _Classname_ {
  constructor(initData, opt) {
    super();
    this.data = initData || {};

    if (opt && opt.client) {
      this.client = opt.client;
    } else {
      this.client = global.EZTableClient;
    }
  }

  static newClient(opt, setDefault) {
    const client = new TableStore.Client(opt);

    if (setDefault) {
      global.EZTableClient = client;
    }
    return client;
  }

  static async init(keys, opt) {
    const client = (opt && opt.client) ? opt.client : global.EZTableClient;

    const classname = this.classname();

    // eslint-disable-next-line no-console
    console.info('Init Table:', classname);

    const pks = keys || [];
    pks.unshift({
      name: 'id',
      type: 'STRING',
    });

    const params = {
      tableMeta: {
        tableName: classname,
        primaryKey: pks,
      },
      reservedThroughput: {
        capacityUnit: {
          read: 0,
          write: 0,
        },
      },
      tableOptions: {
        timeToLive: -1, // 数据的过期时间, 单位秒, -1代表永不过期. 假如设置过期时间为一年, 即为 365 * 24 * 3600.
        maxVersions: 1, // 保存的最大版本数, 设置为1即代表每列上最多保存一个版本(保存最新的版本).
      },
    };

    return new Promise((resolve, reject) => {
      client.createTable(params, (err, data) => {
        // 完成创建, 但是要等几秒钟才能真正创建完成
        if (!err) {
          setTimeout(() => {
            resolve(data);
          }, 2000);
          return;
        }

        // 已经存在,当成功处理
        if (err.code === 409) {
          resolve(data);
          return;
        }

        reject(err);
      });
    });
  }

  // eslint-disable-next-line
  static async destory(keys, opt) {
    // const client = (opt && opt.client) ? opt.client : global.EZTableClient;

    // const classname = this.classname();

    // // eslint-disable-next-line no-console
    // console.info('Destory Table:', classname);

    // TODO: send request
    throw (new Error('Not imp!'));
  }

  static formatParamForRead(data) {
    const ret = {};

    const pks = data.primaryKey;
    for (let i = 0; i < pks.length; i++) {
      const element = pks[i];
      ret[element.name] = element.value;
    }

    const atbs = data.attributes;
    for (let i = 0; i < atbs.length; i++) {
      const element = atbs[i];
      ret[element.columnName] = element.columnValue;
    }
    return ret;
  }

  formatParamForSave() {
    if (!this.data.id) {
      throw (new Error('no value for "id", but it is a MUST have!'));
    }

    const clms = [];
    Object.keys(this.data).forEach((key) => {
      if (key !== 'id') {
        const p = {};
        p[key] = this.data[key];
        clms.push(p);
      }
    });

    return {
      tableName: this.classname(),
      condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
      primaryKey: [{
        id: this.data.id,
      }],
      attributeColumns: clms,
      returnContent: {
        returnType: TableStore.ReturnType.Primarykey,
      },
    };
  }

  async save() {
    return new Promise((resolve, reject) => {
      const params = this.formatParamForSave();
      // console.log('Params:', params);

      this.client.putRow(params, (err, data) => {
        if (err) {
          return reject(err);
        }
        return resolve(data);
      });
    });
  }

  async fetch() {
    const params = {
      tableName: this.classname(),
      primaryKey: [{
        id: this.data.id,
      }],
      maxVersions: 1,
    };
    // const condition = new TableStore.CompositeCondition(TableStore.LogicalOperator.AND);
    // condition.addSubCondition(
    //    new TableStore.SingleColumnCondition('name', 'john', TableStore.ComparatorType.EQUAL));
    // params.columnFilter = condition;
    return new Promise((resolve, reject) => {
      this.client.getRow(params, (err, data) => {
        if (err) {
          return reject(err);
        }
        const ret = this.constructor.formatParamForRead(data.row);
        this.data = ret;
        return resolve(ret);
      });
    });
  }

  static async search(page, size, opt, client) {
    if (page < 1) return [];

    const cli = client || global.EZTableClient;

    const params = {
      tableName: this.classname(),
      indexName: 'name',
      searchQuery: {
        offset: (page - 1) * size,
        limit: size, // 如果只为了取行数，但不需要具体数据，可以设置limit=0，即不返回任意一行数据。
        query: {
          queryType: TableStore.QueryType.MATCH_ALL_QUERY,
        },
        getTotalCount: true, // 结果中的TotalCount可以表示表中数据的总行数， 默认false不返回
      },
      columnToGet: { // 返回列设置：RETURN_SPECIFIED(自定义),RETURN_ALL(所有列),RETURN_NONE(不返回)
        returnType: TableStore.ColumnReturnType.RETURN_ALL,
      },
    };


    return new Promise((resolve, reject) => {
      cli.search(params, (err, data) => {
        // console.log(err, data);

        if (err) {
          return reject(err);
        }

        const ret = {
          totalCounts: data.totalCounts,
          hasMore: data.nextToken !== null,
        }; // this.formatParamForRead(data);
        const items = [];
        for (let i = 0; i < data.rows.length; i++) {
          const element = data.rows[i];
          items.push(this.formatParamForRead(element));
        }
        ret.items = items;
        return resolve(ret);
      });
    });
  }
}

TableDB.TableStore = TableStore;

module.exports = TableDB;
