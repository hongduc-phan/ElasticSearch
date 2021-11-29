const elasticsearch = require('@elastic/elasticsearch')
const logger = require('@sangre-fp/logger').appLogger()
const { isEqual } = require('lodash')
class ElasticClient {
  constructor (endpointURL) {
    console.log(`connecting to ${endpointURL}`)
    this.client = new elasticsearch.Client({
      node: endpointURL
    })
  }

  async searchByIds ({ ids = [], _source = []  }) {
    return this.client.search({
      index: '_all',
      body: {
        query: { ids: { values: ids } },
        _source
      }
    })
  }

  /**
   *
   * @param {string} index
   * @param {string} uuid
   * @param {object} body
   * @return {Promise<>}
   */
  async create (index, uuid, { version, ...doc }) {
    return this.client
      .index({
        body: { ...doc },
        id: uuid,
        index
      })
      .then(({ body }) => body)
      .catch(e => {
        logger.error(`ES: Create failed: ${e.message}`)
        throw e
      })
  }

  /**
   *
   * @param {string} index
   * @param {string} uuid
   * @return {Promise<>}
   */
  async delete (index, uuid) {
    return this.client
      .delete({
        id: uuid,
        index
      })
      .then(({ body }) => body)
      .catch(e => {
        logger.error(`ES: Delete failed: ${e.message}`)
        throw e
      })
  }

  /**
   *
   * @param {string} index
   * @param {string} uuid
   * @param {object} document object
   * @return {Promise<>}
   */
  async update (index, uuid, { version, ...doc}) {
    return this.client
      .update({
        body: { doc },
        index,
        id: uuid
      })
      .then(({ body }) => body)
      .catch(e => {
        logger.error(`ES: Update failed: ${e.message}. Trying to create`)
        return this.create(index, uuid, { version, ...doc }).catch(e => {
          logger.error(`Trying to create failed too: ${e.message}`)
        })
      })
 }

  async updateScript (index, uuid, { source, lang = 'painless', params = {} }) {
    return this.client
      .update({
        index,
        id: uuid,
        body: {
          script: {
            source,
            lang,
            params
          }
        }
      })
      .then(({ body }) => body)
      .catch(e => {
        logger.error(`ES: Update failed: ${e.message}`)
        throw e
      })
 }

  /**
   * If the document exists in ES, the document is updated so, that groups and feed uuid are merged instead of replaced.
   *
   * @param {string} index
   * @param {string} uuid
   * @param {object} body
   * @return {Promise<>}
   */
  async upsert (index, uuid, body) {
    let current
    try {
      current = await this.client.get({
        index,
        id: uuid
      })
    } catch (e) {
      logger.info(`Upsert. Not in elastic: ${uuid}`)
    }

    if (current) {
      if (!isEqual(current._source, body)) {
        const currentDocument = current._source
        // merging props for newsfeed items
        body.groups = Array.from(new Set([...body.groups, ...currentDocument.groups]))
        body.news_feed_uuids = Array.from(new Set([...body.news_feed_uuids, ...currentDocument.news_feed_uuids]))
        return this.update(index, uuid, body)
          .catch(e => {
            logger.warn(`ES: Upsert with strategy update failed. Fallback create: ${e.message}`)
            return this.create(index, type, uuid, body)
              .then(response => response.created)
              .catch(e => {
                logger.error(`ES: Upsert with strategy update fallbacking create failed: ${e.message}`)
                throw e
              })
          })
      } else {
        logger.info(`ES: Document not changed: ${uuid}`)
      }
    } else {
      return this.create(index, uuid, body)
        .catch(e => {
          logger.error(`ES: Upsert failed with strategy create: ${e.message}`)
          throw e
        })
    }
  }
}

module.exports = ElasticClient