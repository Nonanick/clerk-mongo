import { PropertyComparison, Archive, MaybePromise, QueryRequest, QueryResponse, IFilterQuery, implementsFilterComparison, isFilterComparisonArray, FilterComparison, ComparableValues } from 'clerk';
import { FilterQuery, MongoClient, MongoClientOptions } from 'mongodb';
import { BatchInsert } from './procedures';
import { CreateProcedure } from './procedures/model/CreateProcedure';
import { DeleteProcedure } from './procedures/model/DeleteProcedure';
import { UpdateProcedure } from './procedures/model/UpdateProcedure';

export class MongoArchive extends Archive {

  protected _connectPromise: Promise<MongoClient>;

  protected _mongo: MongoClient;

  protected _db: string;

  protected _entityProcedures = new Set([
    BatchInsert
  ]);

  protected _modelProcedures = new Set([
    CreateProcedure,
    UpdateProcedure,
    DeleteProcedure,
  ])

  constructor(options: MongoClientOptions & { database: string; connURL?: string; }) {
    super();

    let connURL = options.connURL ?? process.env.MONGO_URI ?? '';
    this._db = options.database;

    let mongoOpts: any = {
      ...options
    };
    delete mongoOpts.database;
    delete mongoOpts.connURL;

    this._mongo = new MongoClient(
      connURL,
      mongoOpts
    );

    this._connectPromise = this._mongo.connect();

  }

  async connection() {
    return this._connectPromise;
  }

  async query<T = any>(queryRequest: QueryRequest<T>): MaybePromise<QueryResponse<T>> {

    let collection = (await this._connectPromise).db(this._db).collection(queryRequest.source);
    let filtered = collection.find(
      this.parseFilter(queryRequest.filters)
    );

    let projectionProperties: {
      [name: string]: number;
    } = {};

    for (let prop of queryRequest.properties) {
      projectionProperties[prop] = 1;
    }

    if (queryRequest.limit.offset ?? 0 > 0) {
      filtered.skip(queryRequest.limit.offset!);
    }

    filtered
      .project(projectionProperties)
      .limit(queryRequest.limit.amount);


    let response = new QueryResponse(queryRequest);

    try {
      await filtered.forEach(r => {
        response.addRows(r);
      });
    } catch (err) {
      console.error('Failed to query mongo! ', err);
    }

    return response;
  }

  parseFilter(filter: IFilterQuery, mongoFilter?: FilterQuery<any>): FilterQuery<any> {

    const newFilter: FilterQuery<any> = mongoFilter ?? {};

    for (let filterName in filter) {

      let parseFilter = filter[filterName];

      // Either
      if (Array.isArray(parseFilter)) {
        if (isFilterComparisonArray(parseFilter)) {
          if (newFilter.$and == null) {
            newFilter.$and = [];
          }
          newFilter.$and?.push(
            this.parseFilterComparison(parseFilter)
          );
        } else {
          let pushInKey: '$and' | '$or' | "$not" = "$and";
          if (filterName === "$or") {
            pushInKey = "$or";
          }
          if (filterName === "$not") {
            pushInKey = "$not";
          }
          for (let c of parseFilter) {
            if (newFilter[pushInKey] == null) {
              newFilter[pushInKey] = [];
            }
            newFilter[pushInKey].push(
              this.parseFilterComparison(c)
            );
          }
        }
        continue;
      }

      if (implementsFilterComparison(parseFilter)) {
        let pushInKey: '$and' | '$or' | '$not' = "$and";
        if (filterName === "$or") {
          pushInKey = "$or";
        }
        if (filterName === "$not") {
          pushInKey = "$not";
        }
        if (newFilter[pushInKey] == null) {
          newFilter[pushInKey] = [];
        }
        newFilter[pushInKey]!.push(
          this.parseFilterComparison(parseFilter)
        );
      } else {
        let pushInKey: '$and' | '$or' | "$not" = "$and";
        if (filterName === "$or") {
          pushInKey = "$or";
        }
        if (filterName === "$not") {
          pushInKey = "$not";
        }
        if (newFilter[pushInKey] == null) {
          newFilter[pushInKey] = [];
        }

        newFilter[pushInKey].push(
          this.parseFilter(parseFilter!)
        );
      }

    }

    return newFilter;
  }

  parseFilterComparison(cmp: FilterComparison): FilterQuery<any> {
    if (Array.isArray(cmp)) {
      return this.parseComparison(...cmp);
    }
    return this.parseComparison(cmp.property, cmp.comparison, cmp.value);
  }

  parseComparison(property: string, comparison: PropertyComparison, value: ComparableValues): FilterQuery<any> {

    switch (comparison) {
      case "=":
      case "==":
      case "eq":
      case "equal":
        return {
          [property]: value
        };
      case "!=":
      case "not equal":
      case "neq":
      case "<>":
        return {
          [property]: { "$neq": value }
        };
      case "<":
      case "lesser than":
      case "lt":
        return {
          [property]: { "$lt": value }
        };
      case ">":
      case "greater than":
      case "gt":
        return {
          [property]: { $gt: value }
        };
      case "like":
      case "=~":
        return {
          [property]: { "$text": value }
        };
      case "not like":
      case "!=~":
        return {
          [property]: { $not: { "$text": value } }
        };
      case "<=":
      case "lesser than or equal to":
      case "lte":
        return {
          [property]: { "$lte": value }
        };
      case "greater than or equal to":
      case ">=":
      case "gte":
        return {
          [property]: { "$gte": value }
        };
      case "contained in":
      case "in":
      case "included in":
        return {
          [property]: { "$in": value }
        };
      case "not contained in":
      case "not in":
      case "not included in":
        return {
          [property]: { "$nin": value }
        };
      case "is":
        return {
          [property]: { $exists: false }
        };
    }
  }

  async db() {
    return (await this._connectPromise).db(this._db);
  }
}