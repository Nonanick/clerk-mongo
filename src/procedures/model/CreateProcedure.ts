import { IModelProcedure } from "clerk";
import { MongoArchive } from "../../MongoArchive";

export const CreateProcedure: IModelProcedure = {
  name: 'create',
  async execute(archive, req) {

    if (!(archive instanceof MongoArchive)) {
      return new Error('Expects a Mongo Archive!');
    }

    let values = await req.model.$commit();

    return (await archive.db()).collection(req.entity.source).insertOne(values)
      .then(ok => {
        return {
          model: req.model,
          procedure: req.procedure,
          request: req,
          success: true
        };
      }).catch(err => {
        return {
          model: req.model,
          procedure: req.procedure,
          request: req,
          success: false,
          errors: err.message
        };
      });


  }
};