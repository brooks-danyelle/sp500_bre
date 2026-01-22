from setuptools import setup, find_packages
setup(
    name = 'l1_pipeline',
    version = '1.0',
    packages = find_packages(include = ('l1_pipeline*', )) + ['prophecy_config_instances.l1_pipeline'],
    package_dir = {'prophecy_config_instances.l1_pipeline' : 'configs/resources/l1_pipeline'},
    package_data = {'prophecy_config_instances.l1_pipeline' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.9'],
    entry_points = {
'console_scripts' : [
'main = l1_pipeline.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
