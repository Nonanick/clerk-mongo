import { IModelProcedure } from "auria-clerk";
import { MongoArchive } from "../../MongoArchive";

export const UpdateProcedure: IModelProcedure = {
  name: 'update',
  async execute(archive, req) {

    if (!(archive instanceof MongoArchive)) {
      return new Error('Expects a Mongo Archive!');
    }

    let values = await req.model.$commit();
    let id = req.model.$id();

    return (await archive.db()).collection(req.entity.source)
      .findOneAndUpdate(
        {
          [req.entity.identifier.name]: id
        },
        values
      )
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