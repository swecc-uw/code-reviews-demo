import json
import db


def cannonicalize_course_id(course_id):
    """
    Convert course id to format assumed by 'database'
    """
    return course_id.upper()


def main():

    while True:
        print("Select an option:")
        print("1. Get all courses")
        print("2. Get direct prerequisites for a course")
        print("3. Get all prerequisites for a course")
        print("5. Exit")

        option = input("Enter option: ")
        if option == "1":
            courses = db.get_all_courses()
            print("All courses:")
            for course in courses:
                print(course)
        elif option == "2":
            course_id = cannonicalize_course_id(input("Enter course id: "))
            prereqs = db.get_prereqs_for_course(course_id)
            print(f"Prerequisites for course {course_id}:")
            for prereq in prereqs:
                print(prereq)
        elif option == "3":
            course_id = cannonicalize_course_id(input("Enter course id: "))
            filter_course_prefix = input("Enter course prefix to filter (optional): ")
            print(f"All prerequisites for course {course_id}:")
            preset = set()

            def dfs(course_id):
                if course_id in preset or (
                    filter_course_prefix
                    and not course_id.startswith(filter_course_prefix)
                ):
                    return
                preset.add(course_id)
                print(course_id)
                for prereq in db.get_prereqs_for_course(course_id):
                    dfs(prereq)

            dfs(course_id)

            print("All prerequisites for course:")
            print(preset)
        elif option == "4":
            print("Exiting...")
        else:
            print("Invalid option")


if __name__ == "__main__":
    main()
