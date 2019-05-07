module.exports = {
  name: "solutions",
  schema: {
    externalId: String,
    isReusable: Boolean,
    name: String,
    description: String,
    author: String,
    parentId: "ObjectId",
    resourceType: Array,
    language: Array,
    keywords: Array,
    concepts: Array,
    createdFor: Array,
    scoringSystem : String,
    levelToScoreMapping : Object,
    themes: Array,
    questionSequenceByEcm : Object,
    entityTypeId: "ObjectId",
    entityType : String,
    type: String,
    subType : String,
    entities: Array,
    programId: "ObjectId",
    programExternalId: String,
    programName: String,
    programDescription: String,
    startDate: Date,
    endDate: Date,
    status: String,
    evidenceMethods: Object,
    sections: Object,
    registry : Array,
    frameworkId: "ObjectId",
    frameworkExternalId: String,
    parentSolutionId : "ObjectId",
    noOfRatingLevels: Number,
    isRubricDriven: Boolean,
    roles: Object,
    schoolProfileFieldsPerSchoolTypes: Object
  }
};