module.exports = {
    name: "entities",
    schema: {
      entityTypeId: "ObjectId",
      entityType: {
        type: String, 
        index: true
      },
      registryDetails: {
        locationId: { type: String, index: true },
        code: String,
        name:String,
        lastUpdatedAt: Date
      },
      groups: Object,
      metaInformation: {
        externalId: { type: String, index: true },
        name: { type: String, index: true },
      },
      updatedBy: String,
      createdBy: String,
      childHierarchyPath : Array,
      userId : String,
      allowedRoles : Array
    }
  };