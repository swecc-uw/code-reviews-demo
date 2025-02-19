from dataclasses import dataclass
from typing import List, Any, Optional

@dataclass
class CoursePrereqs:
  course_id: str
  course_title: str
  postreqs: List[str] # list of course_ids
  prereqs: List[str] # list of course_ids

@dataclass
class CoursePrereqsResult:
  course_data: List[CoursePrereqs]
  prereq_graph: Any # ignore

def parse_course_prereqs(json_data):
  course_data_raw = json_data.get('course_data', [])
  prereq_graph_raw = json_data.get('prereq_graph', {})

  course_data = []
  for course in course_data_raw:
    prereqs = course.get('prereqs', [])
    postreqs = course.get('postreqs', [])
    prereqs = [prereq.get('course_id', '') for prereq in prereqs]
    postreqs = [postreq.get('course_id', '') for postreq in postreqs]
    course_data.append(
      CoursePrereqs(
        course_id=course.get('course_id', ''),
        course_title=course.get('course_title', ''),
        postreqs=postreqs,
        prereqs=prereqs,
      )
    )

  return CoursePrereqsResult(
    course_data=course_data,
    prereq_graph=prereq_graph_raw,
  )

