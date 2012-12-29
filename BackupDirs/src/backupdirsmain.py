import multiprocessing, os, re, sys, time

from PyQt4 import QtGui, QtCore
from Queue import Queue

# Maybe `/tmp' would be a better choice for default.
settings_file = '%s/.backupdirs' % os.environ.get('HOME', '')
compression_methods = ('gz', 'bz2', 'zip')

class BackupDirsMain(QtGui.QWidget):
  def __init__(self, main = None):
    self.main = main
    self.dirs = [] # Directory list.
    self.archiver = None # The worker thread.
    self.withGui = self.main.withGui
    # Name-value string pairs.
    self.settings = { 'targetDir':os.environ.get('HOME', ''), 'compressionMethod':'bz2', 'fileSizeLimit':'1000', 'fileSuffix':'*' }
    self.defaultTargetDir = self.settings['targetDir']
    self.defaultCompressionMethod = self.settings['compressionMethod']
    self.defaultFileSizeLimit = self.settings['fileSizeLimit']
    self.defaultFileSuffix = self.settings['fileSuffix']
    self.loadSettings()
    if self.withGui:
      QtGui.QWidget.__init__(self)
      tabs = QtGui.QTabWidget()
      tabs.addTab(self.dirListTab(), 'Directories')
      tabs.addTab(self.preferencesTab(), 'Preferences')
      grid = QtGui.QGridLayout(self)
      grid.addWidget(tabs)
      # Timer updates the second column periodically. It's for the GUI only.
      self.timer = Timer(self.dirListWidget)
      if self.dirListWidget.rowCount() == 0:
        self.main.start.setEnabled(False)
   
  def dirListTab(self):
    if not self.withGui:
      return None
    self.dirListWidget = QtGui.QTableWidget(0, 3, self)
    self.dirListWidget.setHorizontalHeaderLabels(['Directory', 'Elapsed Time', 'Status'])
    self.dirListWidget.setSelectionMode(QtGui.QAbstractItemView.MultiSelection)
    for i in range(len(self.dirs)):
      self.dirListWidget.insertRow(self.dirListWidget.rowCount())
      self.dirListWidget.setItem(i, 0, QtGui.QTableWidgetItem(self.dirs[i]))
      elapsedTime = QtGui.QTableWidgetItem('0:00')
      elapsedTime.setFlags(QtCore.Qt.NoItemFlags)
      status = QtGui.QTableWidgetItem('Not started')
      status.setFlags(QtCore.Qt.NoItemFlags)
      self.dirListWidget.setItem(i, 1, elapsedTime)
      self.dirListWidget.setItem(i, 2, status)
    #self.dirListWidget.addItems(self.dirs)
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
    if not self.withGui:
      return None
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
    if not self.withGui:
      return
    selectedItems = self.dirListWidget.selectedItems() 
    for item in selectedItems:
      self.dirs.remove(item.text())
      self.dirListWidget.removeRow(item.row())
      if self.dirListWidget.rowCount() == 0:
        self.main.start.setEnabled(False)
      self.main.setDirty(True)

  def selectTargetDir(self):
    if not self.withGui:
      return
    dialog = QtGui.QFileDialog()
    dialog.setFileMode(QtGui.QFileDialog.DirectoryOnly)
    dialog.setViewMode(QtGui.QFileDialog.Detail);
    if dialog.exec_():
      self.targetDirLine.setText(dialog.selectedFiles()[0])
      self.main.setDirty(True)
    
  def addDirs(self):
    """ Add selected directory to the list. Set `isDirty' flag accordingly.
        Multiple dirs cannot be selected by default in PyQt4... """
    if not self.withGui:
      return False
    dialog = QtGui.QFileDialog()
    dialog.setFileMode(QtGui.QFileDialog.Directory)
    dialog.setViewMode(QtGui.QFileDialog.Detail);
    if dialog.exec_():
      selectedDirs = [str(selDir) for selDir in dialog.selectedFiles()]
      selectedDirsWrong = [selDir for selDir in selectedDirs if len([myDir for myDir in self.dirs if self.isSubDir(selDir, myDir)]) > 0]
      if len(selectedDirsWrong) > 0:
        QtGui.QMessageBox.warning(self, 'Warning',
          'The following directories were skipped. They were subdirectories\n' \
          'of other already set up directories or parent directories of them:\n\n%s'
          % ('\n'.join(selectedDirsWrong)),
          QtGui.QMessageBox.Ok, QtGui.QMessageBox.Ok)
      selectedDirs = [selDir for selDir in selectedDirs if selDir not in selectedDirsWrong]
      for myDir in selectedDirs:
        self.dirs.append(myDir)
        self.dirListWidget.insertRow(self.dirListWidget.rowCount())
        self.dirListWidget.setItem(self.dirListWidget.rowCount() - 1, 0, QtGui.QTableWidgetItem(myDir))
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
            self.dirs.append(line)
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
        if self.withGui:
          for i in range(self.dirListWidget.rowCount()):
            f.write('%s\n' % self.dirListWidget.item(i, 0).text())
        else:
          for d in self.dirs:
            f.write('%s\n' % d)
      if len(self.settings.keys()) > 0:
        f.write('[SETTINGS]\n')
        for name, value in self.settings.items():
          f.write('%s=%s\n' % (name, value))
      f.close()
      self.main.setDirty(False)
    except IOError:
      self.error('Unable to save settings to %s.' % settings_file)

  def error(self, message):
    if self.withGui:
      QtGui.QMessageBox.critical(self, 'Error', message,
                                 QtGui.QMessageBox.Ok, QtGui.QMessageBox.Ok)
    else:
      sys.stderr.write('Error: %s' % message)

  def setStatus(self, mydir, status):
    if not self.withGui:
      return
    for i in range(self.dirListWidget.rowCount()):
      idir = self.dirListWidget.item(i, 0).text()
      if idir == mydir:
        self.dirListWidget.item(i, 2).setText(status)

  def isSubDir(self, aDir, bDir):
    return aDir.startswith(bDir) or bDir.startswith(aDir) 
    
  def canWeStart(self):
    for d in self.dirs:
      if self.isSubDir(d, self.settings['targetDir']):
        return False
    return True

  def startBackup(self):
    if not self.canWeStart():
      self.error('The targetDir %s cannot be in the list of dirs to be archived.'
                 % self.settings['targetDir'])
      return False
    self.archiver = Archiver(self, self.dirs, self.settings)
    self.connect(self.archiver, QtCore.SIGNAL('finishBackup'), self.finishBackup)
    self.connect(self.archiver, QtCore.SIGNAL('setStatus'), self.setStatus)
    self.archiver.start()
    if self.withGui:
      self.timer.start()
    return True
  
  def finishBackup(self):
    QtGui.QMessageBox.information(self, 'Finished',
      'Backup finished successfully. Statistics are saved.', QtGui.QMessageBox.Ok, QtGui.QMessageBox.Ok)
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
    if not self.withGui:
      return
    self.timer.kill()
    if self.archiver:
      self.archiver.stopThread()
      self.archiver.wait() # Similar to join.

class Timer(QtCore.QThread):
  """ Simple timer thread to measure elapsed time and update second column. """
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
    """ Archive the dirs one by one according to settings. The thread can only be cancelled between dirs. """
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
    # Destroy threads carefully with possible restart!
