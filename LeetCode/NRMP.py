from collections import defaultdict

def match(applicants, programs):
    match_list = []
    program_capacity = defaultdict(int)
    applicant_match = defaultdict(str)

    for applicant in applicants:
        for program_preference in applicant['preferences']:
            program = [p for p in programs if p['name'] == program_preference][0]
            if program_capacity[program['name']] < program['capacity']:
                match_list.append((applicant['name'], program['name']))
                program_capacity[program['name']] += 1
                applicant_match[applicant['name']] = program['name']
                break
            elif program['preferences'].index(applicant['name']) < \
                 program['preferences'].index(applicant_match[program['name']]):
                old_applicant = [a for a in applicants if a['name'] == applicant_match[program['name']]][0]
                match_list.remove((old_applicant, program))
                match_list.append((applicant, program))
                applicant_match[applicant['name']] = program['name']
                applicant_match[old_applicant['name']] = ''
                break

    return match_list

applicants = [{'name': 'A', 'preferences': ['X', 'Y', 'Z']},
              {'name': 'B', 'preferences': ['Y', 'X', 'Z']},
              {'name': 'C', 'preferences': ['Z', 'X', 'Y']}]
programs = [{'name': 'X', 'capacity': 1, 'preferences': ['A', 'B', 'C']},
            {'name': 'Y', 'capacity': 1, 'preferences': ['C', 'A', 'B']},
            {'name': 'Z', 'capacity': 1, 'preferences': ['B', 'C', 'A']}]

print(match(applicants, programs))
