-- Find users whose average assessment score is above the course average in every course they are enrolled in.
-- Using a CTE, find users whose total activity count is greater than the average activity count of all users.
-- Using a CTE, identify courses where enrollments exist but no submissions exist.

-- Find users whose average assessment score is above the course average in every course they are enrolled in.
WITH UserCourseAvg AS (
    SELECT
        s.user_id,
        l.course_id,
        AVG(s.marks_scored) AS user_avg_marks
    FROM lms.Assessment_Submission s
    JOIN lms.Assessments a
        ON a.assessment_id = s.assessment_id
    JOIN lms.Lessons l
        ON l.lesson_id = a.lesson_id
    GROUP BY s.user_id, l.course_id
),
CourseAvg AS (
    SELECT
        l.course_id,
        AVG(s.marks_scored) AS course_avg_marks
    FROM lms.Assessment_Submission s
    JOIN lms.Assessments a
        ON a.assessment_id = s.assessment_id
    JOIN lms.Lessons l
        ON l.lesson_id = a.lesson_id
    GROUP BY l.course_id
)
SELECT uca.user_id, ca.course_id
FROM UserCourseAvg uca
JOIN CourseAvg ca
    ON uca.course_id = ca.course_id
GROUP BY uca.user_id, ca.course_id
HAVING MIN(uca.user_avg_marks - ca.course_avg_marks) > 0;
GO

-- Using a CTE, find users whose total activity count is greater than the average activity count of all users.
WITH UserActivityCount AS (
    SELECT
        user_id,
        COUNT(*) AS total_activity_count
    FROM lms.User_Activity
    GROUP BY user_id
),
AvgActivity AS (
    SELECT
        AVG(total_activity_count) AS avg_activity_count
    FROM UserActivityCount
)
SELECT
    u.user_id,
    u.total_activity_count,
    a.avg_activity_count
FROM UserActivityCount u
CROSS JOIN AvgActivity a
WHERE u.total_activity_count > a.avg_activity_count;
GO


-- Using a CTE, identify courses where enrollments exist but no submissions exist.
WITH SubmittedCourses AS (
    SELECT DISTINCT
        l.course_id
    FROM lms.Assessment_Submission s
    JOIN lms.Assessments a
        ON a.assessment_id = s.assessment_id
    JOIN lms.Lessons l
        ON l.lesson_id = a.lesson_id
)
SELECT DISTINCT c.course_id
FROM lms.Courses c
JOIN lms.Enrollments e
    ON e.course_id = c.course_id
LEFT JOIN SubmittedCourses sc
    ON sc.course_id = c.course_id
WHERE sc.course_id IS NULL;
GO
