const presto = require('presto-client');
const {
  map, zipObj, prop, concat
} = require('ramda');
const SqlString = require('sqlstring');

class PrestoDriver {
  constructor(config) {
    this.config = {
      catalog: 'tpch',
      schema: 'sf1',
      user: 'root',
      source:  'nodejs-client',
    };

    this.catalog = this.config.catalog;
    this.client = new presto.Client(this.config);
  }

  async testConnection() {
    const query = SqlString.format('show catalogs like ?', [`%${this.catalog}%`]);

    const result = await this.queryPromised(query);
      if (result.length === 0) {
          throw new Error(`Something went wrong while running query on '${this.catalog}'!`);
      }
    return result;
  }

  query(query, values) {
    const queryWithParams = SqlString.format(query, values);
    return this.queryPromised(queryWithParams);
  }

  queryPromised(query) {
    return new Promise((resolve, reject) => {
      let fullData = [];

      this.client.execute({
        query,
        schema: this.config.schema || 'default',
        data: (error, data, columns) => {
          const normalData = this.normalizeResultOverColumns(data, columns);
          fullData = concat(normalData, fullData);
        },
        success: () => {
          resolve(fullData);
        },
        error: error => {
          reject(new Error(`${error.message}\n${error.error}`));
        }
      });
    });
  }

  normalizeResultOverColumns(data, columns) {
    const columnNames = map(prop('name'), columns || []);
    const arrayToObject = zipObj(columnNames);
    return map(arrayToObject, data || []);
  }
}

module.exports = PrestoDriver;