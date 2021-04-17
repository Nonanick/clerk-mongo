import { IEntityProcedure, IEntityProcedureResponse } from 'clerk';
import { MongoArchive } from '../../MongoArchive';

export const BatchInsert: IEntityProcedure = {
  name: 'batch-insert',
  execute: async (archive, request) => {
    request.context.models;

    if (!(archive instanceof MongoArchive)) {
      return new Error('Procedure "batch-insert" expects a mongo client!');
    }

    if (
      request.context?.models != null
      && Array.isArray(request.context.models)
    ) {

      const batchInsert = await Promise.all(
        request.context.models.map(model => {
          return model.$commit();
        })
      )
        .then(async allValuesToBeInserted => {
          return archive.db()
            .then(async db => {
              return db.collection(request.entity.name)
                .insertMany(allValuesToBeInserted)
                .then(writeResult => {
                  return writeResult.result.ok === writeResult.result.n;
                })
                .catch(insertManyError => {
                  return insertManyError;
                });
            })
            .catch(dbErr => {
              return dbErr;
            })
        })
        .catch(failedToCommitError => {
          return failedToCommitError;
        });

      if (batchInsert instanceof Error) {
        return batchInsert;
      }

      const response: IEntityProcedureResponse = {
        procedure: BatchInsert.name,
        request,
        success: true
      };

      return response;

    } else {
      return new Error('Batch insert expects an array of models as its context! "context : { models : [] }"');
    }

  }
}