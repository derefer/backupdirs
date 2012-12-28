#!/usr/bin/env python

import sys

from PyQt4 import QtGui, QtCore

from backupdirsmain import BackupDirsMain

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
      'http://derefer.org <derefer@gmail.com>\n' \
      'Version 0.2, 2012-12-28\n\n' \
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

if __name__ == '__main__':
  app = QtGui.QApplication(sys.argv)
  main = BackupDirs()
  main.show()
  sys.exit(app.exec_())
