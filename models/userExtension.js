module.exports = {
    name: "userExtension",
    schema: {
      externalId: {
        type: String,
        required: true
      },
      userId: {
        type: String,
        required: true
      },
      roles: Array,
      createdBy: {
        type: String,
        required: true
      },
      updatedBy: {
        type: String,
        required: true
      },
      status: {
        type: String,
        default: "active"
      },
      isDeleted: {
        type: Boolean,
        default: false
      },
      removedFromHomeScreen: Array,
      improvementProjects : {
        type : Array,
        default : []
      }
    }
  }