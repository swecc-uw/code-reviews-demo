import model
import json

db = None
with open('./sample_prereq.json', 'r') as f:
  db = model.parse_course_prereqs(json.load(f))

def get_all_courses():
    return [f'{course.course_id} - {course.course_title}' for course in db.course_data]

def get_prereqs_for_course(course_id):
    for course in db.course_data:
        if course.course_id == course_id:
            return course.prereqs
    return []