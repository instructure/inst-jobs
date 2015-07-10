(function () {
    var Delayed = {};

    Delayed.Render = {};
    Delayed.Render.attempts = function (previousAttempts, callType, jobData) {
        var thisAttempt = parseInt(previousAttempts, 10) + 1;
        switch(callType) {
            case "display":
                return thisAttempt + "/" + jobData.max_attempts;
            default:
                return thisAttempt;
        }
    };

    Delayed.Render.runTime = function (lockedAt, callType) {
        var elapsed, days, hours, minutes, seconds, remaining, formattedTime;
        elapsed = Math.round((new Date().getTime() - Date.parse(lockedAt)) / 1000);
        switch(callType) {
            case "display": {
                days = Math.floor(elapsed / 86400);
                remaining = elapsed - (days * 86400);
                hours = Math.floor(remaining / 3600);
                remaining = remaining - (hours * 3600);
                minutes = Math.floor(remaining / 60);
                seconds = remaining - (minutes * 60);
                formattedTime = ""
                if(days > 0) {
                    formattedTime = days + "d "
                }
                formattedTime = formattedTime + ("0" + hours).slice(-2) + ":" + ("0" + minutes).slice(-2) + ":" + ("0" + seconds).slice(-2);
                return formattedTime;
            }
            default:
                return elapsed;
        }
    };

    $(document).ready(function () {
        var runningTable, runningInterval, tagsTable, tagsInterval, jobsTable, jobsInterval
        runningTable = $('#running').DataTable({
            "autoWidth": false,
            "bSort": false,
            "paging": false,
            "processing": true,
            "searching": false,
            "scrollY": "200px",
            "serverSide": true,
            "order": [[5, "desc"]],
            "ajax": ENV.Routes.running,
            "columns": [
                {"data": "id"},
                {"data": "locked_by", "className": "worker"},
                {"data": "tag", "className": "tag"},
                {"data": "attempts", "render": Delayed.Render.attempts, "className": "attempts"},
                {"data": "strand", "className": "strand"},
                {"data": "locked_at", "render": Delayed.Render.runTime}
            ]
        });
        runningInterval = setInterval(function () { runningTable.ajax.reload(); }, 2000);

        tagsTable = $("#tags").DataTable({
            "autoWidth": false,
            "bSort": false,
            "paging": false,
            "processing": true,
            "searching": false,
            "scrollY": "200px",
            "order": [[1, "desc"]],
            "ajax": ENV.Routes.tags,
            "columns": [
                {"data": "tag", "className": "tag"},
                {"data": "count"}
            ]
        });
        tagsInterval = setInterval(function () { tagsTable.ajax.reload(); }, 10000);

        jobsTable = $("#jobs").DataTable({
            "autoWidth": false,
            "bSort": false,
            "paging": true,
            "processing": true,
            "searching": false,
            "scrollY": "200px",
            "serverSide": true,
            "order": [[1, "desc"]],
            "ajax": {
                "url": ENV.Routes.jobs,
                "data": function (data) {
                    data.flavor      = $("select#current_jobs_flavor").val();
                    data.search_term = $("input#jobs_search_term").val();
                }
            },
            "columns": [
                {"data": "id"},
                {"data": "tag", "className": "tag"},
                {"data": "attempts", "render": Delayed.Render.attempts},
                {"data": "priority"},
                {"data": "strand"},
                {"data": "run_at"}
            ]
        });

        $('body').on('click', '.refresh_jobs_link', function(event) {
            event.preventDefault();
            jobsTable.ajax.reload();
            return true
        });

        $('body').on('change', 'select#current_jobs_flavor', function (event) {
            event.preventDefault();
            $selectBox = $(this);
            $selectedOption = $($selectBox.children(':selected'));
            $searchDiv = $('div#job_search');

            if ($selectedOption.data('requires-search')) {
                $searchDiv.show();
            } else {
                $searchDiv.hide();
            }

            jobsTable.ajax.reload();
            return true
        });

        $('body').on('keyup', '#jobs_search_term', function (event) {
            if (event.keyCode == 13) {
                jobsTable.ajax.reload();
            }
            return true
        });
    });
})();
