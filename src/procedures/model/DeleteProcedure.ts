import { IModelProcedure } from "clerk";
import { MongoArchive } from "../../MongoArchive";

export const DeleteProcedure: IModelProcedure = {
  name: 'delete',
  async execute(archive, req) {

    if (!(archive instanceof MongoArchive)) {
      return new Error('Expects a Mongo Archive!');
    }

    let id = req.model.$id();

    return (await archive.db()).collection(req.entity.source)
      .findOneAndDelete(
        {
          [req.entity.identifier.name]: id
        }
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