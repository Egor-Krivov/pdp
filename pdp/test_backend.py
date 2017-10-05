import unittest

from .backend import THREAD, PROCESS, Backend, check_backend, choose_backend


class TestBackend(unittest.TestCase):
    def test_backend_equalities(self):
        self.assertIs(THREAD, Backend.THREAD)
        self.assertEqual(THREAD, Backend.THREAD)
        self.assertIs(PROCESS, Backend.PROCESS)
        #self.assertEqual(PROCESS, Backend.PROCESS)

    def test_check_backend(self):
        with self.assertRaises(TypeError):
            check_backend('thread')
        check_backend(THREAD)
        #check_backend(PROCESS)

    def test_choose_backend(self):
        with self.assertRaises(TypeError):
            choose_backend('thread', True, False)
        self.assertTrue(choose_backend(THREAD, True, False))
        #self.assertFalse(choose_backend(PROCESS, True, False))
