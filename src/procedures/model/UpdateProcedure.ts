import { IModelProcedure } from "clerk";
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
        { [req.entity.identifier.name]: id },
        { $set: values, }
      )
      .then(modified => {
        return {
          model: req.model,
          procedure: req.procedure,
          request: req,
          success: modified.ok ? true : false
        };
      })
      .catch(err => {
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