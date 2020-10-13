# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot


snapshots = Snapshot()

snapshots[
    "test_add_columns_ok 1"
] = """- full_name: ALGERIA IN AFRICA
- full_name: ARGENTINA IN AMERICA
- full_name: BRAZIL IN AMERICA
- full_name: CANADA IN AMERICA
- full_name: EGYPT IN MIDDLE EAST
- full_name: ETHIOPIA IN AFRICA
- full_name: FRANCE IN EUROPE
- full_name: GERMANY IN EUROPE
- full_name: INDIA IN ASIA
- full_name: INDONESIA IN ASIA
- full_name: IRAN IN MIDDLE EAST
- full_name: IRAQ IN MIDDLE EAST
- full_name: JAPAN IN ASIA
- full_name: JORDAN IN MIDDLE EAST
- full_name: KENYA IN AFRICA
- full_name: MOROCCO IN AFRICA
- full_name: MOZAMBIQUE IN AFRICA
- full_name: PERU IN AMERICA
- full_name: CHINA IN ASIA
- full_name: ROMANIA IN EUROPE
- full_name: SAUDI ARABIA IN MIDDLE EAST
- full_name: VIETNAM IN ASIA
- full_name: RUSSIA IN EUROPE
- full_name: UNITED KINGDOM IN EUROPE
- full_name: UNITED STATES IN AMERICA
"""

snapshots[
    "test_add_integer_literals_ok 1"
] = """42
...
"""

snapshots[
    "test_add_string_literals_ok 1"
] = """Hello, World!
...
"""

snapshots[
    "test_and_literals_ok 1"
] = """false
...
"""

snapshots[
    "test_back_nav_region_nation_name_ok 1"
] = """- ALGERIA
- ARGENTINA
- BRAZIL
- CANADA
- EGYPT
- ETHIOPIA
- FRANCE
- GERMANY
- INDIA
- INDONESIA
- IRAN
- IRAQ
- JAPAN
- JORDAN
- KENYA
- MOROCCO
- MOZAMBIQUE
- PERU
- CHINA
- ROMANIA
- SAUDI ARABIA
- VIETNAM
- RUSSIA
- UNITED KINGDOM
- UNITED STATES
"""

snapshots[
    "test_back_nav_region_nation_ok 1"
] = """- (ALGERIA)
- (ARGENTINA)
- (BRAZIL)
- (CANADA)
- (EGYPT)
- (ETHIOPIA)
- (FRANCE)
- (GERMANY)
- (INDIA)
- (INDONESIA)
- (IRAN)
- (IRAQ)
- (JAPAN)
- (JORDAN)
- (KENYA)
- (MOROCCO)
- (MOZAMBIQUE)
- (PERU)
- (CHINA)
- (ROMANIA)
- ("SAUDI ARABIA")
- (VIETNAM)
- (RUSSIA)
- ("UNITED KINGDOM")
- ("UNITED STATES")
"""

snapshots[
    "test_compose_nation_name_ok 1"
] = """- ALGERIA
- ARGENTINA
- BRAZIL
- CANADA
- EGYPT
- ETHIOPIA
- FRANCE
- GERMANY
- INDIA
- INDONESIA
- IRAN
- IRAQ
- JAPAN
- JORDAN
- KENYA
- MOROCCO
- MOZAMBIQUE
- PERU
- CHINA
- ROMANIA
- SAUDI ARABIA
- VIETNAM
- RUSSIA
- UNITED KINGDOM
- UNITED STATES
"""

snapshots[
    "test_compose_nation_region_name_ok 1"
] = """- AFRICA
- AMERICA
- AMERICA
- AMERICA
- MIDDLE EAST
- AFRICA
- EUROPE
- EUROPE
- ASIA
- ASIA
- MIDDLE EAST
- MIDDLE EAST
- ASIA
- MIDDLE EAST
- AFRICA
- AFRICA
- AFRICA
- AMERICA
- ASIA
- EUROPE
- MIDDLE EAST
- ASIA
- EUROPE
- EUROPE
- AMERICA
"""

snapshots[
    "test_compose_nation_select_ok 1"
] = """- nation_name: ALGERIA
  region_name: AFRICA
- nation_name: ARGENTINA
  region_name: AMERICA
- nation_name: BRAZIL
  region_name: AMERICA
- nation_name: CANADA
  region_name: AMERICA
- nation_name: EGYPT
  region_name: MIDDLE EAST
- nation_name: ETHIOPIA
  region_name: AFRICA
- nation_name: FRANCE
  region_name: EUROPE
- nation_name: GERMANY
  region_name: EUROPE
- nation_name: INDIA
  region_name: ASIA
- nation_name: INDONESIA
  region_name: ASIA
- nation_name: IRAN
  region_name: MIDDLE EAST
- nation_name: IRAQ
  region_name: MIDDLE EAST
- nation_name: JAPAN
  region_name: ASIA
- nation_name: JORDAN
  region_name: MIDDLE EAST
- nation_name: KENYA
  region_name: AFRICA
- nation_name: MOROCCO
  region_name: AFRICA
- nation_name: MOZAMBIQUE
  region_name: AFRICA
- nation_name: PERU
  region_name: AMERICA
- nation_name: CHINA
  region_name: ASIA
- nation_name: ROMANIA
  region_name: EUROPE
- nation_name: SAUDI ARABIA
  region_name: MIDDLE EAST
- nation_name: VIETNAM
  region_name: ASIA
- nation_name: RUSSIA
  region_name: EUROPE
- nation_name: UNITED KINGDOM
  region_name: EUROPE
- nation_name: UNITED STATES
  region_name: AMERICA
"""

snapshots[
    "test_count_nation_region_ok 1"
] = """- 25
"""

snapshots[
    "test_count_region_ok 1"
] = """- 5
"""

snapshots[
    "test_count_region_select_nation_count_ok 1"
] = """- nation_count: 5
- nation_count: 5
- nation_count: 5
- nation_count: 5
- nation_count: 5
"""

snapshots[
    "test_count_region_via_opend_ok 1"
] = """- 5
"""

snapshots[
    "test_date_literal_nav_ok 1"
] = """2020.0
...
"""

snapshots[
    "test_date_literal_ok 1"
] = """2020-01-02
...
"""

snapshots[
    "test_filter_customer_by_region_name_then_count_ok 1"
] = """- 243
"""

snapshots[
    "test_filter_nation_by_region_name_ok 1"
] = """- (ALGERIA)
- (ETHIOPIA)
- (KENYA)
- (MOROCCO)
- (MOZAMBIQUE)
"""

snapshots[
    "test_filter_nation_by_region_name_then_nav_column_ok 1"
] = """- ALGERIA
- ETHIOPIA
- KENYA
- MOROCCO
- MOZAMBIQUE
"""

snapshots[
    "test_filter_region_by_name_ok 1"
] = """- (AFRICA)
"""

snapshots[
    "test_filter_region_by_name_then_nav_ok 1"
] = """- AFRICA
"""

snapshots[
    "test_filter_region_by_name_then_select_ok 1"
] = """- name: AFRICA
  nation_names:
  - ALGERIA
  - ETHIOPIA
  - KENYA
  - MOROCCO
  - MOZAMBIQUE
"""

snapshots[
    "test_filter_region_by_nation_count_ok 1"
] = """- (AFRICA)
- (AMERICA)
- (ASIA)
- (EUROPE)
- ("MIDDLE EAST")
"""

snapshots[
    "test_filter_region_name_ok 1"
] = """- (AFRICA)
"""

snapshots[
    "test_filter_region_true_ok 1"
] = """[]
"""

snapshots[
    "test_json_literal_nav_ok 1"
] = """- world
"""

snapshots[
    "test_json_literal_nested_nav_ok 1"
] = """'YES'
"""

snapshots[
    "test_json_literal_ok 1"
] = """hello:
- world
"""

snapshots[
    "test_literal_boolean_ok 1"
] = """true
...
"""

snapshots[
    "test_literal_composition_with_another_literal_ok 1"
] = """false
...
"""

snapshots[
    "test_literal_composition_with_query_via_dot_ok 1"
] = """- true
- true
- true
- true
- true
"""

snapshots[
    "test_literal_composition_with_query_via_op_ok 1"
] = """- true
- true
- true
- true
- true
"""

snapshots[
    "test_literal_integer_ok 1"
] = """42
...
"""

snapshots[
    "test_literal_string_ok 1"
] = """Hello
...
"""

snapshots[
    "test_mul_integer_literals_ok 1"
] = """44
...
"""

snapshots[
    "test_nav_nation_name_ok 1"
] = """- ALGERIA
- ARGENTINA
- BRAZIL
- CANADA
- EGYPT
- ETHIOPIA
- FRANCE
- GERMANY
- INDIA
- INDONESIA
- IRAN
- IRAQ
- JAPAN
- JORDAN
- KENYA
- MOROCCO
- MOZAMBIQUE
- PERU
- CHINA
- ROMANIA
- SAUDI ARABIA
- VIETNAM
- RUSSIA
- UNITED KINGDOM
- UNITED STATES
"""

snapshots[
    "test_nav_nation_ok 1"
] = """- (ALGERIA)
- (ARGENTINA)
- (BRAZIL)
- (CANADA)
- (EGYPT)
- (ETHIOPIA)
- (FRANCE)
- (GERMANY)
- (INDIA)
- (INDONESIA)
- (IRAN)
- (IRAQ)
- (JAPAN)
- (JORDAN)
- (KENYA)
- (MOROCCO)
- (MOZAMBIQUE)
- (PERU)
- (CHINA)
- (ROMANIA)
- ("SAUDI ARABIA")
- (VIETNAM)
- (RUSSIA)
- ("UNITED KINGDOM")
- ("UNITED STATES")
"""

snapshots[
    "test_nav_nation_region_name_ok 1"
] = """- AFRICA
- AMERICA
- AMERICA
- AMERICA
- MIDDLE EAST
- AFRICA
- EUROPE
- EUROPE
- ASIA
- ASIA
- MIDDLE EAST
- MIDDLE EAST
- ASIA
- MIDDLE EAST
- AFRICA
- AFRICA
- AFRICA
- AMERICA
- ASIA
- EUROPE
- MIDDLE EAST
- ASIA
- EUROPE
- EUROPE
- AMERICA
"""

snapshots[
    "test_or_literals_ok 1"
] = """true
...
"""

snapshots[
    "test_select_back_nav_ok 1"
] = """- nation_names:
  - ALGERIA
  - ETHIOPIA
  - KENYA
  - MOROCCO
  - MOZAMBIQUE
- nation_names:
  - ARGENTINA
  - BRAZIL
  - CANADA
  - PERU
  - UNITED STATES
- nation_names:
  - INDIA
  - INDONESIA
  - JAPAN
  - CHINA
  - VIETNAM
- nation_names:
  - FRANCE
  - GERMANY
  - ROMANIA
  - RUSSIA
  - UNITED KINGDOM
- nation_names:
  - EGYPT
  - IRAN
  - IRAQ
  - JORDAN
  - SAUDI ARABIA
"""

snapshots[
    "test_select_nav_select_nav_column_ok 1"
] = """- AFRICA
- AMERICA
- ASIA
- EUROPE
- MIDDLE EAST
"""

snapshots[
    "test_select_nav_select_nav_multi_ok 1"
] = """- name: ALGERIA
  region_comment: 'lar deposits. blithely final packages cajole. regular waters are
    final requests. regular accounts are according to '
  region_name: AFRICA
- name: ARGENTINA
  region_comment: hs use ironic, even requests. s
  region_name: AMERICA
- name: BRAZIL
  region_comment: hs use ironic, even requests. s
  region_name: AMERICA
- name: CANADA
  region_comment: hs use ironic, even requests. s
  region_name: AMERICA
- name: EGYPT
  region_comment: uickly special accounts cajole carefully blithely close requests.
    carefully final asymptotes haggle furiousl
  region_name: MIDDLE EAST
- name: ETHIOPIA
  region_comment: 'lar deposits. blithely final packages cajole. regular waters are
    final requests. regular accounts are according to '
  region_name: AFRICA
- name: FRANCE
  region_comment: ly final courts cajole furiously final excuse
  region_name: EUROPE
- name: GERMANY
  region_comment: ly final courts cajole furiously final excuse
  region_name: EUROPE
- name: INDIA
  region_comment: ges. thinly even pinto beans ca
  region_name: ASIA
- name: INDONESIA
  region_comment: ges. thinly even pinto beans ca
  region_name: ASIA
- name: IRAN
  region_comment: uickly special accounts cajole carefully blithely close requests.
    carefully final asymptotes haggle furiousl
  region_name: MIDDLE EAST
- name: IRAQ
  region_comment: uickly special accounts cajole carefully blithely close requests.
    carefully final asymptotes haggle furiousl
  region_name: MIDDLE EAST
- name: JAPAN
  region_comment: ges. thinly even pinto beans ca
  region_name: ASIA
- name: JORDAN
  region_comment: uickly special accounts cajole carefully blithely close requests.
    carefully final asymptotes haggle furiousl
  region_name: MIDDLE EAST
- name: KENYA
  region_comment: 'lar deposits. blithely final packages cajole. regular waters are
    final requests. regular accounts are according to '
  region_name: AFRICA
- name: MOROCCO
  region_comment: 'lar deposits. blithely final packages cajole. regular waters are
    final requests. regular accounts are according to '
  region_name: AFRICA
- name: MOZAMBIQUE
  region_comment: 'lar deposits. blithely final packages cajole. regular waters are
    final requests. regular accounts are according to '
  region_name: AFRICA
- name: PERU
  region_comment: hs use ironic, even requests. s
  region_name: AMERICA
- name: CHINA
  region_comment: ges. thinly even pinto beans ca
  region_name: ASIA
- name: ROMANIA
  region_comment: ly final courts cajole furiously final excuse
  region_name: EUROPE
- name: SAUDI ARABIA
  region_comment: uickly special accounts cajole carefully blithely close requests.
    carefully final asymptotes haggle furiousl
  region_name: MIDDLE EAST
- name: VIETNAM
  region_comment: ges. thinly even pinto beans ca
  region_name: ASIA
- name: RUSSIA
  region_comment: ly final courts cajole furiously final excuse
  region_name: EUROPE
- name: UNITED KINGDOM
  region_comment: ly final courts cajole furiously final excuse
  region_name: EUROPE
- name: UNITED STATES
  region_comment: hs use ironic, even requests. s
  region_name: AMERICA
"""

snapshots[
    "test_select_nav_select_nav_only_ok 1"
] = """- region_name: AFRICA
- region_name: AMERICA
- region_name: AMERICA
- region_name: AMERICA
- region_name: MIDDLE EAST
- region_name: AFRICA
- region_name: EUROPE
- region_name: EUROPE
- region_name: ASIA
- region_name: ASIA
- region_name: MIDDLE EAST
- region_name: MIDDLE EAST
- region_name: ASIA
- region_name: MIDDLE EAST
- region_name: AFRICA
- region_name: AFRICA
- region_name: AFRICA
- region_name: AMERICA
- region_name: ASIA
- region_name: EUROPE
- region_name: MIDDLE EAST
- region_name: ASIA
- region_name: EUROPE
- region_name: EUROPE
- region_name: AMERICA
"""

snapshots[
    "test_select_nav_select_nav_select_ok 1"
] = """- region:
    name: AFRICA
- region:
    name: AMERICA
- region:
    name: AMERICA
- region:
    name: AMERICA
- region:
    name: MIDDLE EAST
- region:
    name: AFRICA
- region:
    name: EUROPE
- region:
    name: EUROPE
- region:
    name: ASIA
- region:
    name: ASIA
- region:
    name: MIDDLE EAST
- region:
    name: MIDDLE EAST
- region:
    name: ASIA
- region:
    name: MIDDLE EAST
- region:
    name: AFRICA
- region:
    name: AFRICA
- region:
    name: AFRICA
- region:
    name: AMERICA
- region:
    name: ASIA
- region:
    name: EUROPE
- region:
    name: MIDDLE EAST
- region:
    name: ASIA
- region:
    name: EUROPE
- region:
    name: EUROPE
- region:
    name: AMERICA
"""

snapshots[
    "test_select_nav_select_nav_table_ok 1"
] = """- ALGERIA
- ARGENTINA
- BRAZIL
- CANADA
- EGYPT
- ETHIOPIA
- FRANCE
- GERMANY
- INDIA
- INDONESIA
- IRAN
- IRAQ
- JAPAN
- JORDAN
- KENYA
- MOROCCO
- MOZAMBIQUE
- PERU
- CHINA
- ROMANIA
- SAUDI ARABIA
- VIETNAM
- RUSSIA
- UNITED KINGDOM
- UNITED STATES
"""

snapshots[
    "test_select_nav_select_ok 1"
] = """- comment: ' haggle. carefully final deposits detect slyly agai'
  name: ALGERIA
- comment: al foxes promise slyly according to the regular accounts. bold requests
    alon
  name: ARGENTINA
- comment: 'y alongside of the pending deposits. carefully special packages are about
    the ironic forges. slyly special '
  name: BRAZIL
- comment: eas hang ironic, silent packages. slyly regular packages are furiously
    over the tithes. fluffily bold
  name: CANADA
- comment: y above the carefully unusual theodolites. final dugouts are quickly across
    the furiously regular d
  name: EGYPT
- comment: ven packages wake quickly. regu
  name: ETHIOPIA
- comment: refully final requests. regular, ironi
  name: FRANCE
- comment: 'l platelets. regular accounts x-ray: unusual, regular acco'
  name: GERMANY
- comment: ss excuses cajole slyly across the packages. deposits print aroun
  name: INDIA
- comment: ' slyly express asymptotes. regular deposits haggle slyly. carefully ironic
    hockey players sleep blithely. carefull'
  name: INDONESIA
- comment: 'efully alongside of the slyly final dependencies. '
  name: IRAN
- comment: nic deposits boost atop the quickly final requests? quickly regula
  name: IRAQ
- comment: ously. final, express gifts cajole a
  name: JAPAN
- comment: ic deposits are blithely about the carefully regular pa
  name: JORDAN
- comment: ' pending excuses haggle furiously deposits. pending, express pinto beans
    wake fluffily past t'
  name: KENYA
- comment: rns. blithely bold courts among the closely regular packages use furiously
    bold platelets?
  name: MOROCCO
- comment: s. ironic, unusual asymptotes wake blithely r
  name: MOZAMBIQUE
- comment: platelets. blithely pending dependencies use fluffily across the even pinto
    beans. carefully silent accoun
  name: PERU
- comment: c dependencies. furiously express notornis sleep slyly regular accounts.
    ideas sleep. depos
  name: CHINA
- comment: ular asymptotes are about the furious multipliers. express dependencies
    nag above the ironically ironic account
  name: ROMANIA
- comment: ts. silent requests haggle. closely express packages sleep across the blithely
  name: SAUDI ARABIA
- comment: 'hely enticingly express accounts. even, final '
  name: VIETNAM
- comment: ' requests against the platelets use never according to the quickly regular
    pint'
  name: RUSSIA
- comment: eans boost carefully special requests. accounts are. carefull
  name: UNITED KINGDOM
- comment: y final packages. slow foxes cajole quickly. quickly silent platelets breach
    ironic accounts. unusual pinto be
  name: UNITED STATES
"""

snapshots[
    "test_select_nav_select_select_ok 1"
] = """- nn:
  - ALGERIA
  - ETHIOPIA
  - KENYA
  - MOROCCO
  - MOZAMBIQUE
- nn:
  - ARGENTINA
  - BRAZIL
  - CANADA
  - PERU
  - UNITED STATES
- nn:
  - INDIA
  - INDONESIA
  - JAPAN
  - CHINA
  - VIETNAM
- nn:
  - FRANCE
  - GERMANY
  - ROMANIA
  - RUSSIA
  - UNITED KINGDOM
- nn:
  - EGYPT
  - IRAN
  - IRAQ
  - JORDAN
  - SAUDI ARABIA
"""

snapshots[
    "test_select_nav_select_select_select_nav_ok 1"
] = """- ALGERIA
- ARGENTINA
- BRAZIL
- CANADA
- EGYPT
- ETHIOPIA
- FRANCE
- GERMANY
- INDIA
- INDONESIA
- IRAN
- IRAQ
- JAPAN
- JORDAN
- KENYA
- MOROCCO
- MOZAMBIQUE
- PERU
- CHINA
- ROMANIA
- SAUDI ARABIA
- VIETNAM
- RUSSIA
- UNITED KINGDOM
- UNITED STATES
"""

snapshots[
    "test_select_nav_select_select_select_ok 1"
] = """- nnn:
  - ALGERIA
  - ETHIOPIA
  - KENYA
  - MOROCCO
  - MOZAMBIQUE
- nnn:
  - ARGENTINA
  - BRAZIL
  - CANADA
  - PERU
  - UNITED STATES
- nnn:
  - INDIA
  - INDONESIA
  - JAPAN
  - CHINA
  - VIETNAM
- nnn:
  - FRANCE
  - GERMANY
  - ROMANIA
  - RUSSIA
  - UNITED KINGDOM
- nnn:
  - EGYPT
  - IRAN
  - IRAQ
  - JORDAN
  - SAUDI ARABIA
"""

snapshots[
    "test_select_select_multiple_ok 1"
] = """nation_names:
- ALGERIA
- ARGENTINA
- BRAZIL
- CANADA
- EGYPT
- ETHIOPIA
- FRANCE
- GERMANY
- INDIA
- INDONESIA
- IRAN
- IRAQ
- JAPAN
- JORDAN
- KENYA
- MOROCCO
- MOZAMBIQUE
- PERU
- CHINA
- ROMANIA
- SAUDI ARABIA
- VIETNAM
- RUSSIA
- UNITED KINGDOM
- UNITED STATES
region_names:
- AFRICA
- AMERICA
- ASIA
- EUROPE
- MIDDLE EAST
"""

snapshots[
    "test_select_select_nav_nav_ok 1"
] = """region_names:
- AFRICA
- AMERICA
- AMERICA
- AMERICA
- MIDDLE EAST
- AFRICA
- EUROPE
- EUROPE
- ASIA
- ASIA
- MIDDLE EAST
- MIDDLE EAST
- ASIA
- MIDDLE EAST
- AFRICA
- AFRICA
- AFRICA
- AMERICA
- ASIA
- EUROPE
- MIDDLE EAST
- ASIA
- EUROPE
- EUROPE
- AMERICA
"""

snapshots[
    "test_select_select_nav_one_ok 1"
] = """region_names:
- AFRICA
- AMERICA
- ASIA
- EUROPE
- MIDDLE EAST
"""

snapshots[
    "test_select_tables_ok 1"
] = """region:
- (AFRICA)
- (AMERICA)
- (ASIA)
- (EUROPE)
- ("MIDDLE EAST")
"""

snapshots[
    "test_sub_integer_literals_ok 1"
] = """42
...
"""

snapshots[
    "test_take_region_nation_ok 1"
] = """- (ALGERIA)
- (ARGENTINA)
"""

snapshots[
    "test_take_region_ok 1"
] = """- (AFRICA)
- (AMERICA)
"""

snapshots[
    "test_take_region_x_nation_ok 1"
] = """- (ALGERIA)
- (ARGENTINA)
- (BRAZIL)
- (CANADA)
- (ETHIOPIA)
- (KENYA)
- (MOROCCO)
- (MOZAMBIQUE)
- (PERU)
- ("UNITED STATES")
"""

snapshots[
    "test_truediv_integer_literals_ok 1"
] = """44
...
"""
