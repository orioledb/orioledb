#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres

class EngineDiagnosticsTest(unittest.TestCase):
    def test_diagnostics_lifecycle(self):
        with testgres.get_new_node('diag_node') as node:
            node.init()
            # OrioleDB shared_preload_libraries içinde olmalı
            node.append_conf('postgresql.conf', "shared_preload_libraries = 'orioledb'\n")
            node.append_conf('postgresql.conf', "orioledb.main_buffers = 16MB\n")
            node.start()
            
            # Eklentiyi kur
            node.safe_psql('postgres', 'CREATE EXTENSION orioledb;')
            
            # Fonksiyonu test et
            res = node.safe_psql('postgres', 'SELECT orioledb_engine_status();').decode('utf-8').strip()
            self.assertEqual(res, 'ORIOLEDB_ACTIVE_V1_OK')
            print("\n[OK] orioledb_engine_status basariyla calisti!")

if __name__ == '__main__':
    unittest.main()