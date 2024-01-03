import uuid

from data_diff.abcs.database_types import String_UUID
from data_diff.databases import MySQL
from data_diff.table_segment import create_mesh_from_points
from data_diff.utils import ArithUUID, safezip
from tests.common import DiffTestCase, table_segment


# We do not need real tables, just any reference to them for proper object creation.
class TestDiffMesh(DiffTestCase):
    db_cls = MySQL

    def test_meta_parameters_passed_from_coltypes_to_values(self):
        key_types1 = [String_UUID(lowercase=True, uppercase=False)]
        key_types2 = [String_UUID(lowercase=False, uppercase=True)]

        # side B is wider than side A to ensure there are "outer" regions.
        min_uuid1 = uuid.UUID("11111111-1111-1111-1111-111111111111")
        max_uuid1 = uuid.UUID("EEEEEEEE-EEEE-EEEE-EEEE-EEEEEEEEEEEE")
        min_uuid2 = uuid.UUID("00000000-0000-0000-0000-000000000000")
        max_uuid2 = uuid.UUID("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")
        min_key1 = (key_types1[0].make_value(min_uuid1),)
        max_key1 = (key_types1[0].make_value(max_uuid1),)
        min_key2 = (key_types2[0].make_value(min_uuid2),)
        max_key2 = (key_types2[0].make_value(max_uuid2),)

        # Verify that we pass the meta-parameters from col types to values:
        assert isinstance(min_key1[0], ArithUUID)
        assert isinstance(max_key1[0], ArithUUID)
        assert isinstance(min_key2[0], ArithUUID)
        assert isinstance(max_key2[0], ArithUUID)
        assert min_key1[0].uuid == min_uuid1
        assert min_key1[0].lowercase == True
        assert min_key1[0].uppercase == False
        assert max_key1[0].uuid == max_uuid1
        assert max_key1[0].lowercase == True
        assert max_key1[0].uppercase == False
        assert min_key2[0].uuid == min_uuid2
        assert min_key2[0].lowercase == False
        assert min_key2[0].uppercase == True
        assert max_key2[0].uuid == max_uuid2
        assert max_key2[0].lowercase == False
        assert max_key2[0].uppercase == True

    def test_meta_parameters_left_as_is_if_not_casted(self):
        table1 = table_segment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        key_types1 = [String_UUID(lowercase=True, uppercase=False)]

        min_uuid1 = uuid.UUID("11111111-1111-1111-1111-111111111111")
        max_uuid1 = uuid.UUID("EEEEEEEE-EEEE-EEEE-EEEE-EEEEEEEEEEEE")
        min_key1 = (key_types1[0].make_value(min_uuid1),)
        max_key1 = (key_types1[0].make_value(max_uuid1),)

        btable1 = table1.new_key_bounds(min_key=min_key1, max_key=max_key1)
        assert btable1.min_key[0] is min_key1[0]  # by identity, not by equality
        assert btable1.max_key[0] is max_key1[0]  # by identity, not by equality

    def test_mesh_keys_meta_parameters_preserved(self):
        table1 = table_segment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        table2 = table_segment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        key_types1 = [String_UUID(lowercase=True, uppercase=False)]
        key_types2 = [String_UUID(lowercase=False, uppercase=True)]

        # side B is wider than side A to ensure there are "outer" regions.
        min_uuid1 = uuid.UUID("11111111-1111-1111-1111-111111111111")
        max_uuid1 = uuid.UUID("EEEEEEEE-EEEE-EEEE-EEEE-EEEEEEEEEEEE")
        min_uuid2 = uuid.UUID("00000000-0000-0000-0000-000000000000")
        max_uuid2 = uuid.UUID("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")
        min_key1 = (key_types1[0].make_value(min_uuid1),)
        max_key1 = (key_types1[0].make_value(max_uuid1),)
        min_key2 = (key_types2[0].make_value(min_uuid2),)
        max_key2 = (key_types2[0].make_value(max_uuid2),)

        # This is what TableDiffer._bisect_and_diff_tables() does, precisely (yes, using key1!):
        btable1 = table1.new_key_bounds(min_key=min_key1, max_key=max_key1, key_types=key_types1)
        btable2 = table2.new_key_bounds(min_key=min_key1, max_key=max_key1, key_types=key_types2)

        # Verify that both sides have proper (the side-specific) pk meta-parameters:
        assert btable1.min_key[0].uuid == min_uuid1
        assert btable1.min_key[0].lowercase == True
        assert btable1.min_key[0].uppercase == False
        assert btable1.max_key[0].uuid == max_uuid1
        assert btable1.max_key[0].lowercase == True
        assert btable1.max_key[0].uppercase == False
        assert btable2.min_key[0].uuid == min_uuid1
        assert btable2.min_key[0].lowercase == False
        assert btable2.min_key[0].uppercase == True
        assert btable2.max_key[0].uuid == max_uuid1
        assert btable2.max_key[0].lowercase == False
        assert btable2.max_key[0].uppercase == True

        # This is what TableDiffer._bisect_and_diff_tables() does, precisely:
        points = [list(sorted(p)) for p in safezip(min_key1, min_key2, max_key1, max_key2)]
        box_mesh = create_mesh_from_points(*points)
        new_regions = [(p1, p2) for p1, p2 in box_mesh if p1 < p2 and not (p1 >= min_key1 and p2 <= max_key1)]
        extra_tables = [
            (
                table1.new_key_bounds(min_key=p1, max_key=p2, key_types=key_types1),
                table2.new_key_bounds(min_key=p1, max_key=p2, key_types=key_types2),
            )
            for p1, p2 in new_regions
        ]

        # Verify that extra ("outer") segments have the proper pk meta-parameters:
        assert len(extra_tables) == 2

        assert extra_tables[0][0].min_key[0].uuid == min_uuid2
        assert extra_tables[0][0].min_key[0].lowercase == True
        assert extra_tables[0][0].min_key[0].uppercase == False
        assert extra_tables[0][0].max_key[0].uuid == min_uuid1
        assert extra_tables[0][0].max_key[0].lowercase == True
        assert extra_tables[0][0].max_key[0].uppercase == False
        assert extra_tables[0][1].min_key[0].uuid == min_uuid2
        assert extra_tables[0][1].min_key[0].lowercase == False
        assert extra_tables[0][1].min_key[0].uppercase == True
        assert extra_tables[0][1].max_key[0].uuid == min_uuid1
        assert extra_tables[0][1].max_key[0].lowercase == False
        assert extra_tables[0][1].max_key[0].uppercase == True

        assert extra_tables[1][0].min_key[0].uuid == max_uuid1
        assert extra_tables[1][0].min_key[0].lowercase == True
        assert extra_tables[1][0].min_key[0].uppercase == False
        assert extra_tables[1][0].max_key[0].uuid == max_uuid2
        assert extra_tables[1][0].max_key[0].lowercase == True
        assert extra_tables[1][0].max_key[0].uppercase == False
        assert extra_tables[1][1].min_key[0].uuid == max_uuid1
        assert extra_tables[1][1].min_key[0].lowercase == False
        assert extra_tables[1][1].min_key[0].uppercase == True
        assert extra_tables[1][1].max_key[0].uuid == max_uuid2
        assert extra_tables[1][1].max_key[0].lowercase == False
        assert extra_tables[1][1].max_key[0].uppercase == True
