module.exports = (req) => {

    let entityValidator = {

        add: function () {
            req.checkParams('_id').exists().withMessage("required solution id")
            req.checkBody('data').exists().withMessage("required data")
        },
        metaForm: function () {
            req.checkParams('_id').exists().withMessage("required observation id")
        },
        addEntityToObservation: function () {
            req.checkParams('_id').exists().withMessage("required observation id")
        },
        removeEntityFromObservation: function () {
            req.checkParams('_id').exists().withMessage("required observation id")
        },
        create: function () {
            req.checkQuery('solutionId').exists().withMessage("required solution id")
            req.checkBody('data').exists().withMessage("required data")
        },
        search: function () {
            req.checkQuery('search').exists().withMessage("required search text")
        },
        assessment: function () {
            req.checkParams('_id').exists().withMessage("required observation id")
            req.checkQuery('entityId').exists().withMessage("required entity id")
        },
        complete: function () {
            req.checkParams('_id').exists().withMessage("required observation id")
        },
        update: function () {
            req.checkParams('_id').exists().withMessage("required observation id")
        },
        details : function () {
            req.checkParams('_id').exists().withMessage("required observation id")
        },
        completedObservations : function () {
            req.checkQuery('fromDate').exists().withMessage("required from date");
        }

    }

    if (entityValidator[req.params.method]) entityValidator[req.params.method]();

};