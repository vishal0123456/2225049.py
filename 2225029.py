import pandas as pd
import re

def find_absent_streaks(attendance_df):
    """
    This function identifies students who were absent for more than 3 consecutive days.
    It returns a DataFrame with the latest absence streak for each student.
    """
    # Sort data by student_id and date
    attendance_df = attendance_df.sort_values(by=['student_id', 'attendance_date'])
    
    # Shift the attendance_date column for previous and next day calculations
    attendance_df['prev_date'] = attendance_df.groupby('student_id')['attendance_date'].shift(1)
    attendance_df['next_date'] = attendance_df.groupby('student_id')['attendance_date'].shift(-1)

    # Calculate gaps between consecutive attendance records
    attendance_df['gap_prev'] = (attendance_df['attendance_date'] - attendance_df['prev_date']).dt.days
    attendance_df['gap_next'] = (attendance_df['next_date'] - attendance_df['attendance_date']).dt.days

    # Identify absence start and end points
    attendance_df['absence_start'] = (attendance_df['gap_prev'] != 1)
    attendance_df['absence_end'] = (attendance_df['gap_next'] != 1)

    # Filter only 'Absent' records
    absent_records = attendance_df[attendance_df['status'] == 'Absent'].copy()

    # Determine absence streak start and end dates
    absent_records['absence_start_date'] = absent_records['attendance_date'].where(absent_records['absence_start'])
    absent_records['absence_end_date'] = absent_records['attendance_date'].where(absent_records['absence_end'])

    # Fill missing values to propagate streak start and end dates
    absent_records['absence_start_date'].fillna(method='ffill', inplace=True)
    absent_records['absence_end_date'].fillna(method='bfill', inplace=True)

    # Group by student_id and absence streaks
    streaks = absent_records.groupby(['student_id', 'absence_start_date', 'absence_end_date']).size().reset_index(name='total_absent_days')

    # Filter only the latest absence streak for each student
    latest_streak = streaks.loc[streaks.groupby('student_id')['absence_start_date'].idxmax()]

    return latest_streak

def is_valid_email(email):
    """
    Validates the email address using regex.
    Email must follow: username@domain.com format.
    """
    pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*@[a-zA-Z]+\.com$'
    return bool(re.match(pattern, email))

def run(attendance_df, students_df):
    """
    Main function to process attendance data and generate the required output.
    """
    # Step 1: Identify students with long absence streaks
    absent_streaks = find_absent_streaks(attendance_df)
    absent_streaks = absent_streaks[absent_streaks['total_absent_days'] > 3]

    # Step 2: Merge with students data to get parent email and student name
    result = absent_streaks.merge(students_df[['student_id', 'student_name', 'parent_email']], on='student_id', how='left')

    # Step 3: Validate email addresses
    result['email'] = result['parent_email'].apply(lambda x: x if is_valid_email(x) else None)

    # Step 4: Generate message for valid emails
    result['msg'] = result.apply(lambda row: (
        f"Dear Parent, your child {row['student_name']} was absent from {row['absence_start_date'].strftime('%Y-%m-%d')} to {row['absence_end_date'].strftime('%Y-%m-%d')} "
        f"for {row['total_absent_days']} days. Please ensure their attendance improves."
    ) if row['email'] else None, axis=1)

    # Return the final DataFrame
    return result[['student_id', 'absence_start_date', 'absence_end_date', 'total_absent_days', 'email', 'msg']]

