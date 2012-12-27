#!/usr/bin/env python

import os, re, sys, time
import multiprocessing
from Queue import Queue
from PyQt4 import QtGui, QtCore

# Maybe `/tmp' would be a better choice for default.
settings_file = '%s/.backupdirs' % os.environ.get('HOME', '')
compression_methods = ('gz', 'bz2', 'zip')

class BackupDirs(QtGui.QMainWindow):
  def __init__(self):
    QtGui.QMainWindow.__init__(self)
    self.initUI()
    
  def initUI(self):
    self.isDirty = False # Settings file needs to be updated.
    self.isArchiving = False # Still running.
    self.create_menus()
    self.setWindowTitle('Backup')
    self.setWindowIcon(QtGui.QIcon('icons/logo.png'))
    self.resize(640, 480)
    self.center()
    self.main = BackupDirsMain(self)
    self.setCentralWidget(self.main)
    
  def create_menus(self):
    self.start = QtGui.QAction(QtGui.QIcon('icons/start.png'), 'St&art', self)
    self.connect(self.start, QtCore.SIGNAL('triggered()'), self.startBackup)
    self.start.setShortcut('Ctrl+A')
    self.start.setStatusTip('Start archiving')
    self.stop = QtGui.QAction(QtGui.QIcon('icons/stop.png'), 'St&op', self)
    self.connect(self.stop, QtCore.SIGNAL('triggered()'), self.stopBackup)
    self.stop.setShortcut('Ctrl+X')
    self.stop.setStatusTip('Stop archiving')
    self.stop.setEnabled(False)
    self.save = QtGui.QAction(QtGui.QIcon('icons/save.png'), '&Save', self)
    self.connect(self.save, QtCore.SIGNAL('triggered()'), self.storeSettings)
    self.save.setShortcut('Ctrl+S')
    self.save.setStatusTip('Save preferences')
    self.save.setEnabled(False)
    self.reset = QtGui.QAction(QtGui.QIcon('icons/reset.png'), '&Reset', self)
    self.connect(self.reset, QtCore.SIGNAL('triggered()'), self.loadSettings)
    self.reset.setShortcut('Ctrl+R')
    self.reset.setStatusTip('Reset preferences')
    self.reset.setEnabled(False)

    myexit = QtGui.QAction(QtGui.QIcon('icons/exit.png'), 'E&xit', self)
    myexit.setShortcut('Ctrl+Q')
    myexit.setStatusTip('Exit application')
    self.connect(myexit, QtCore.SIGNAL('triggered()'), QtCore.SLOT("close()"))
    about = QtGui.QAction(QtGui.QIcon('icons/about.png'), '&About', self)
    about.setShortcut('Ctrl+B')
    about.setStatusTip('About')
    self.connect(about, QtCore.SIGNAL('triggered()'), self.about)

    self.statusBar()

    menubar = self.menuBar()
    myfile = menubar.addMenu('&File')
    myfile.addAction(self.start)
    myfile.addAction(self.stop)
    myfile.addSeparator()
    myfile.addAction(self.save)
    myfile.addAction(self.reset)
    myfile.addSeparator()
    myfile.addAction(myexit)
    myhelp = menubar.addMenu('&Help')
    myhelp.addAction(about)
    
    toolbar = self.addToolBar('')
    toolbar.addAction(self.start)
    toolbar.addAction(self.stop)
  
  def startBackup(self):
    """ Starting worker child in different thread, it'll notify us if it's ready. """
    if self.main.startBackup():
      self.start.setEnabled(False)
      self.stop.setEnabled(True)
      self.isArchiving = True
    
  def stopBackup(self):
    """ Calling the child to cancel the job. """
    self.main.stopBackup()
    self.start.setEnabled(True)
    self.stop.setEnabled(False)
    self.isArchiving = False    

  def finishBackup(self):
    """ Emitted by the child process. """
    self.start.setEnabled(True)
    self.stop.setEnabled(False)
    self.isArchiving = False    
  
  def closeEvent(self, event):
    if self.isDirty:
      reply = QtGui.QMessageBox.question(self, 'Exit', 'Abandoning changes?',
        QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.No)
      if reply == QtGui.QMessageBox.No:
        self.storeSettings()
      event.accept()
    elif self.isArchiving:
      reply = QtGui.QMessageBox.question(self,
        'Exit', 'Archiving is still running. Really quit?',
        QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.No)
      if reply == QtGui.QMessageBox.Yes:
        self.stopBackup()
      event.ignore()
    event.accept()
    
  def storeSettings(self):
    self.save.setEnabled(False)
    self.reset.setEnabled(False)
    self.main.storeSettings()
    
  def loadSettings(self):
    if self.isDirty:
      reply = QtGui.QMessageBox.question(self, 'Reset', 'Abandoning changes?',
        QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.No)
      if reply == QtGui.QMessageBox.No:
        return
    self.save.setEnabled(False)
    self.reset.setEnabled(False)
    self.main.loadSettings()
    
  def setDirty(self, isDirty):
    self.isDirty = isDirty
    if self.isDirty:
      self.save.setEnabled(True)
      self.reset.setEnabled(True)
    
  def center(self):
    screen = QtGui.QDesktopWidget().screenGeometry()
    size = self.geometry()
    self.move((screen.width() - size.width()) / 2, (screen.height() - size.height()) / 2)

  def about(self):
    QtGui.QMessageBox.information(self, 'About',
      'Basic backup application written in PyQt4 by Ferenc Kovacs.\n' \
      'http://derefer.uw.hu <derefer@gmail.com>\n' \
      'Version 0.1, 2011-12-04\n\n' \
      'Current features:\n' \
      '- Preferences stored to ~/.backupdirs\n' \
      '- Archiving directories into a specified folder\n' \
      '- Each folder will have its own archive file\n' \
      '- Adjustable compression method\n' \
      '- All files exceeding a given size will be ignored\n' \
      '- Interruptible, asynchronous backups\n' \
      '- Progress indication\n' \
      '- File suffix filtering\n\n' \
      'Work in progress:\n' \
      '- Summary generation\n' \
      '- Multi-threaded archiving\n' \
      '- Command line mode', QtGui.QMessageBox.Ok) 

class BackupDirsMain(QtGui.QWidget):
  def __init__(self, main = None):
    super(BackupDirsMain, self).__init__()
    self.main = main
    self.directories = [] # Directory list.
    self.archiver = None # The worker thread.
    # Name-value string pairs.
    self.settings = { 'targetDir':os.environ.get('HOME', ''), 'compressionMethod':'bz2', 'fileSizeLimit':'1000', 'fileSuffix':'*' }
    self.defaultTargetDir = self.settings['targetDir']
    self.defaultCompressionMethod = self.settings['compressionMethod']
    self.defaultFileSizeLimit = self.settings['fileSizeLimit']
    self.defaultFileSuffix = self.settings['fileSuffix']
    self.loadSettings()
    tabs = QtGui.QTabWidget()
    tabs.addTab(self.dirListTab(), 'Directories')
    tabs.addTab(self.preferencesTab(), 'Preferences')
    grid = QtGui.QGridLayout(self)
    grid.addWidget(tabs)
    # Updates the second row periodically.
    self.timer = Timer(self.dirListWidget)
    if self.dirListWidget.rowCount() == 0:
      self.main.start.setEnabled(False)
   
  def dirListTab(self):
    self.dirListWidget = QtGui.QTableWidget(0, 3, self)
    self.dirListWidget.setHorizontalHeaderLabels(['Directory', 'Elapsed Time', 'Status'])
    self.dirListWidget.setSelectionMode(QtGui.QAbstractItemView.MultiSelection)
    for i in range(len(self.directories)):
      self.dirListWidget.insertRow(self.dirListWidget.rowCount())
      self.dirListWidget.setItem(i, 0, QtGui.QTableWidgetItem(self.directories[i]))
      elapsedTime = QtGui.QTableWidgetItem('0:00')
      elapsedTime.setFlags(QtCore.Qt.NoItemFlags)
      status = QtGui.QTableWidgetItem('Not started')
      status.setFlags(QtCore.Qt.NoItemFlags)
      self.dirListWidget.setItem(i, 1, elapsedTime)
      self.dirListWidget.setItem(i, 2, status)
    #self.dirListWidget.addItems(self.directories)
    self.dirListWidget.horizontalHeader().setResizeMode(0, QtGui.QHeaderView.ResizeToContents)
    self.dirListWidget.horizontalHeader().setResizeMode(1, QtGui.QHeaderView.Stretch)
    self.dirListWidget.horizontalHeader().setResizeMode(2, QtGui.QHeaderView.Stretch)
    self.addButton = QtGui.QPushButton(QtGui.QIcon('icons/add.png'), 'Add')
    self.removeButton = QtGui.QPushButton(QtGui.QIcon('icons/remove.png'), 'Remove')
    buttonsLayout = QtGui.QHBoxLayout()
    buttonsLayout.addWidget(self.addButton)
    buttonsLayout.addWidget(self.removeButton)
    self.connect(self.addButton, QtCore.SIGNAL('pressed()'), self.addDirs)
    self.connect(self.removeButton, QtCore.SIGNAL('pressed()'), self.removeDirs)
    dirListLayout = QtGui.QVBoxLayout()
    dirListLayout.addWidget(self.dirListWidget)
    dirListLayout.addLayout(buttonsLayout)
    dirListTab = QtGui.QWidget()
    dirListTab.setLayout(dirListLayout)
    return dirListTab    

  def preferencesTab(self):
    preferencesLayout = QtGui.QGridLayout()
    preferencesLayout.addWidget(QtGui.QLabel('Target directory:'), 0, 0)
    targetDirLayout = QtGui.QHBoxLayout()
    self.targetDirLine = QtGui.QLineEdit()
    self.targetDirLine.setText(self.settings['targetDir'])
    self.connect(self.targetDirLine, QtCore.SIGNAL('textChanged(QString)'), self.setTargetDir) 
    self.browseButton = QtGui.QPushButton('Browse')
    self.connect(self.browseButton, QtCore.SIGNAL('pressed()'), self.selectTargetDir)
    targetDirLayout.addWidget(self.targetDirLine)
    targetDirLayout.addWidget(self.browseButton)
    preferencesLayout.addLayout(targetDirLayout, 0, 1)
    preferencesLayout.addWidget(QtGui.QLabel('Compression method:'), 1, 0)
    self.compressionMethodsCombo = QtGui.QComboBox()
    self.compressionMethodsCombo.addItems(compression_methods)
    if self.settings['compressionMethod'] not in compression_methods:
      QtGui.QMessageBox.warning(self, 'Warning',
        'Invalid value "%s" read for compressionMethod from %s. Restoring defaults...'
        % (self.settings['compressionMethod'], settings_file), QtGui.QMessageBox.Ok, QtGui.QMessageBox.Ok)
      self.setCompression(self.defaultCompressionMethod)
      # Allow the user to re-save the file.
      self.main.setDirty(True)
    else:  
      self.compressionMethodsCombo.setCurrentIndex(compression_methods.index(self.settings['compressionMethod']))
    self.connect(self.compressionMethodsCombo, QtCore.SIGNAL('currentIndexChanged(QString)'), self.setCompression)
    preferencesLayout.addWidget(self.compressionMethodsCombo, 1, 1)
    preferencesLayout.addWidget(QtGui.QLabel('File size limit:'), 2, 0)
    self.fileSizeLimitSpin = QtGui.QSpinBox()
    self.fileSizeLimitSpin.setRange(100, 10000)
    try:
      self.fileSizeLimitSpin.setValue(int(self.settings['fileSizeLimit']))
    except ValueError:
      QtGui.QMessageBox.warning(self, 'Warning',
        'Invalid value "%s" read for fileSizeLimit from %s. Restoring defaults...'
        % (self.settings['fileSizeLimit'], settings_file), QtGui.QMessageBox.Ok, QtGui.QMessageBox.Ok)
      self.setFileSizeLimit(self.defaultFileSizeLimit)
      # Allow the user to re-save the file.
      self.main.setDirty(True)
    self.connect(self.fileSizeLimitSpin, QtCore.SIGNAL('valueChanged(QString)'), self.setFileSizeLimit)
    preferencesLayout.addWidget(self.fileSizeLimitSpin, 2, 1)
    self.fileSuffixLine = QtGui.QLineEdit()
    self.fileSuffixLine.setText(self.settings['fileSuffix'])
    self.connect(self.fileSuffixLine, QtCore.SIGNAL('textChanged(QString)'), self.setFileSuffix)
    preferencesLayout.addWidget(QtGui.QLabel('File suffix:'), 3, 0)
    preferencesLayout.addWidget(self.fileSuffixLine, 3, 1)
    preferencesLayout.setRowStretch(preferencesLayout.rowCount(), 1)
    preferencesTab = QtGui.QWidget()
    preferencesTab.setLayout(preferencesLayout)
    return preferencesTab
  
  def setTargetDir(self, targetDir):
    self.settings['targetDir'] = targetDir
    self.main.setDirty(True)
  
  def setCompression(self, compressionMethod):
    self.settings['compressionMethod'] = compressionMethod;
    self.main.setDirty(True)

  def setFileSizeLimit(self, fileSizeLimit):
    self.settings['fileSizeLimit'] = fileSizeLimit
    self.main.setDirty(True)

  def setFileSuffix(self, fileSuffix):
    self.settings['fileSuffix'] = fileSuffix
    self.main.setDirty(True)

  def removeDirs(self):
    selectedItems = self.dirListWidget.selectedItems() 
    for item in selectedItems:
      self.dirListWidget.removeRow(item.row())
      if self.dirListWidget.rowCount() == 0:
        self.main.start.setEnabled(False)
      self.main.setDirty(True)

  def selectTargetDir(self):
    dialog = QtGui.QFileDialog()
    dialog.setFileMode(QtGui.QFileDialog.DirectoryOnly)
    dialog.setViewMode(QtGui.QFileDialog.Detail);
    if dialog.exec_():
      self.targetDirLine.setText(dialog.selectedFiles()[0])
      self.main.setDirty(True)
    
  def addDirs(self):
    """ Add selected directory to the list. Set `isDirty' flag accordingly.
        Multiple directories cannot be selected by default in PyQt4... """
    dialog = QtGui.QFileDialog()
    dialog.setFileMode(QtGui.QFileDialog.Directory)
    dialog.setViewMode(QtGui.QFileDialog.Detail);
    if dialog.exec_():
      itemsBefore = self.dirListWidget.rowCount()
      selectedDirs = filter(lambda mydir: mydir not in [self.dirListWidget.item(i, 0).text() for i in range(itemsBefore)], dialog.selectedFiles())
      for mydir in selectedDirs:
        self.dirListWidget.insertRow(self.dirListWidget.rowCount())
        self.dirListWidget.setItem(self.dirListWidget.rowCount() - 1, 0, QtGui.QTableWidgetItem(mydir))
        elapsedTime = QtGui.QTableWidgetItem('0:00')
        elapsedTime.setFlags(QtCore.Qt.NoItemFlags)
        self.dirListWidget.setItem(self.dirListWidget.rowCount() - 1, 1, elapsedTime)
        status = QtGui.QTableWidgetItem('Not started')
        status.setFlags(QtCore.Qt.NoItemFlags)
        self.dirListWidget.setItem(self.dirListWidget.rowCount() - 1, 2, status)
        self.main.start.setEnabled(True)
        self.main.setDirty(True)
     
  def loadSettings(self):
    try:
      f = open(settings_file, 'r')
      reading_directories = False
      reading_settings = False
      for line in f:
        line = line.strip()
        if re.match('^\[DIRECTORIES\]$', line):
          reading_directories = True
          reading_settings = False
        elif re.match('\[SETTINGS\]$', line):
          reading_settings = True
          reading_directories = False
        else:
          if reading_directories:
            self.directories.append(line)
          elif reading_settings:
            splitted = line.split('=')
            if len(splitted) != 2:
              return
            self.settings[splitted[0]] = splitted[1]
      f.close()
    except IOError:
      self.storeSettings()

  def storeSettings(self):
    try:
      f = open(settings_file, 'w')
      if self.dirListWidget.rowCount() > 0:
        f.write('[DIRECTORIES]\n')
        for i in range(self.dirListWidget.rowCount()):
          f.write('%s\n' % self.dirListWidget.item(i, 0).text())
      if len(self.settings.keys()) > 0:
        f.write('[SETTINGS]\n')
        for name, value in self.settings.items():
          f.write('%s=%s\n' % (name, value))
      f.close()
      self.main.setDirty(False)
      self.settings.keys()
    except IOError:
      QtGui.QMessageBox.critical(self, 'Error',
        'Unable to save settings to %s.'
        % settings_file, QtGui.QMessageBox.Ok, QtGui.QMessageBox.Ok)

  def setStatus(self, mydir, status):
    for i in range(self.dirListWidget.rowCount()):
      idir = self.dirListWidget.item(i, 0).text()
      if idir == mydir:
        self.dirListWidget.item(i, 2).setText(status)

  def canWeStart(self):
    for i in range(self.dirListWidget.rowCount()):
      mydir = self.dirListWidget.item(i, 0).text()
      if mydir.startsWith(self.settings['targetDir']):
        return False
    return True

  def startBackup(self):
    if not self.canWeStart():
      QtGui.QMessageBox.critical(self, 'Error',
        'The targetDir %s cannot be in the list of directories to be archived.'
        % self.settings['targetDir'], QtGui.QMessageBox.Ok, QtGui.QMessageBox.Ok)
      self.main.stopBackup()
      return False
    self.timer.start()
    self.archiver = Archiver(self, self.directories, self.settings)
    self.connect(self.archiver, QtCore.SIGNAL('finishBackup'), self.finishBackup)
    self.connect(self.archiver, QtCore.SIGNAL('setStatus'), self.setStatus)
    self.archiver.start()
    return True
  
  def finishBackup(self):
    QtGui.QMessageBox.information(self, 'Finished',
      'Backup finished successfully. The statistics will be saved.', QtGui.QMessageBox.Ok, QtGui.QMessageBox.Ok)
    f = open(os.path.join(self.archiver.getTargetDir(), 'README'), 'w')
    for i in range(self.dirListWidget.rowCount()):
      #save them first to a file with du!!
      mydir = self.dirListWidget.item(i, 0).text()
      time = self.dirListWidget.item(i, 1).text()
      size = os.popen('du -shk %s' % mydir).readlines()[0].split('\t')[0]
      f.write('%s\t%s\t%s\n' % (mydir, size, time))
      elapsedTime = QtGui.QTableWidgetItem('0:00')
      elapsedTime.setFlags(QtCore.Qt.NoItemFlags)
      status = QtGui.QTableWidgetItem('Not started')
      status.setFlags(QtCore.Qt.NoItemFlags)
      self.dirListWidget.setItem(i, 1, elapsedTime)
      self.dirListWidget.setItem(i, 2, status)
    f.close()
    self.main.finishBackup()
  
  def stopBackup(self):
    self.timer.kill()
    if self.archiver:
      self.archiver.stopThread()
      self.archiver.wait() # Similar to join.

class Timer(QtCore.QThread):
  """ Simple timer thread to measure elapsed time. """
  def __init__(self, tableWidget):
    QtCore.QThread.__init__(self)
    self.table = tableWidget
    self.alive = True

  def kill(self):
    self.alive = False
    
  def increase(self, time):
    t = time.split(':')
    mins, secs = int(t[0]), int(t[1])
    if secs == 59:
      mins += 1
      secs = 0
    else:
      secs += 1
    return '%d:%02d' % (mins, secs)
    
  def run(self):
    while self.alive:
      for i in range(self.table.rowCount()):
        if self.table.item(i, 2).text() == 'Started':
          self.table.item(i, 1).setText(self.increase(self.table.item(i, 1).text()))
      time.sleep(1)

class Archiver(QtCore.QThread):
  def __init__(self, main, dirs, settings):
    """ Archive the directories one by one according to settings. The thread can only be cancelled between directories. """
    QtCore.QThread.__init__(self)
    self.threadPool = ThreadPool(self, multiprocessing.cpu_count() + 1)
    self.stopNow = False
    self.dirs = dirs
    self.dirSizes = []
    self.targetDir = '%s/backup-%s' % (settings['targetDir'], time.strftime('%Y%m%d_%H%M%S'))
    os.popen('mkdir -p %s' % self.targetDir)
    self.fileSizeLimit = settings['fileSizeLimit']
    self.compressionMethod = settings['compressionMethod']
    self.fileSuffix = settings['fileSuffix']
    self.calculateDirSizes()
    
  def getTargetDir(self):
    return self.targetDir
  
  def calculateDirSizes(self):
    for mydir in self.dirs:
      self.dirSizes.append(int(os.popen('du -sk %s' % mydir).readlines()[0].split('\t')[0]))
  
  def stopThread(self):
    self.stopNow = True # kill workers too!
  
  def run(self):
    for mydir in self.dirs:
      if self.stopNow:
        return
      argsToPass = { 'dir':mydir, 'targetDir':self.targetDir, 'fileSuffix':self.fileSuffix,
        'fileSizeLimit':self.fileSizeLimit, 'compressionMethod':self.compressionMethod }
      self.threadPool.add_task(argsToPass)
      self.emit(QtCore.SIGNAL('setStatus'), mydir, 'Scheduled')
    self.threadPool.wait_completion()
    self.emit(QtCore.SIGNAL('finishBackup'))
    
  def setStatus(self, mydir, status):
    # This chain is a bit long!
    self.emit(QtCore.SIGNAL('setStatus'), mydir, status)

class Worker(QtCore.QThread):
  def __init__(self, tasks):
    QtCore.QThread.__init__(self)
    self.tasks = tasks
    # Python's daemon threads would be a bit easier, but not supported by QThread.
    self.alive = True
    self.start()
  
  def kill(self):
    self.alive = False
    
  def run(self):
    while self.alive:
      args = self.tasks.get()
      # Start clock and update GUI!
      self.emit(QtCore.SIGNAL('setStatus'), args['dir'], 'Started')
      targetFile = '%s/%s.tar' % (args['targetDir'], args['dir'].split('/')[-1])
      # Avoid absolute paths and -P/-C tricks with tar.
      print 'cd %s && find . -type f -name \'%s\' -size -%dk -print0 | xargs -0 tar cf %s' \
        % (args['dir'], args['fileSuffix'], int(args['fileSizeLimit']), targetFile)
      os.popen('cd %s && find . -type f -name \'%s\' -size -%dk -print0 | xargs -0 tar cf %s'
        % (args['dir'], args['fileSuffix'], int(args['fileSizeLimit']), targetFile))
      if args['compressionMethod'] == 'zip':
        os.popen('zip %s' % targetFile)
      elif args['compressionMethod'] == 'gz':
        os.popen('gzip %s' % targetFile)
      elif args['compressionMethod'] == 'bz2':
        os.popen('bzip2 %s' % targetFile)
      self.tasks.task_done()
      # Stop clock and update GUI!
      self.emit(QtCore.SIGNAL('setStatus'), args['dir'], 'Completed')
      
class ThreadPool:
  def __init__(self, main, size):
    self.main = main
    self.size = size
    self.tasks = Queue(size)
    self.threads = []
    for _ in range(size):
      worker = Worker(self.tasks)
      main.connect(worker, QtCore.SIGNAL('setStatus'), self.main.setStatus)
      self.threads.append(worker)
    
  def add_task(self, args):
    # Non-blocking addition to the task list.
    self.tasks.put(args)
  
  def wait_completion(self):
    # All of the tasks completed.
    self.tasks.join()
    # Destroy threads carefully with possible restart!!!!

if __name__ == '__main__':
  app = QtGui.QApplication(sys.argv)
  main = BackupDirs()
  main.show()
  sys.exit(app.exec_())
