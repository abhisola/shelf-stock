#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import division
import cv2
import sys
import json
import numpy as np
import math


class TriRecognizeParams:
    def __init__(self):
        self.useBounds = False
        self.useSharpen = False
        self.outputState = False
        self.useThreshold = True
        self.useDistort = False
        self.useStaticThreshold = False
        self.equalizeHist = False
        self.minArcLength = 80
        self.maxArcLength = 30000
        self.minArea = 500
        self.maxArea = 50000
        self.polyApproxFactor = 3.5
        self.minLegLength = 10
        self.maxLegLength = 100
        self.maxLegVar = 100
        self.heightLegRatio = 2.5
        self.legRatio = -1
        self.baseTriangleCount = 252
        self.staticThreshold = 128
        self.bounds = []
        self.distortCoeffs = np.zeros((8, 1), np.float64)
        self.colorCorr = False

    def _set_defaults(self):
        self.useDistort = True
        # Default distortion "-0.00002,0.0000000004,0.00006,0.00006"
        self.distortCoeffs[0, 0] = float("-0.00002")
        self.distortCoeffs[1, 0] = float("0.0000000004")
        self.distortCoeffs[2, 0] = float("0.00006")
        self.distortCoeffs[3, 0] = float("0.00006")
        self.minArcLength = int('45')
        self.maxArcLength = int('200')
        self.minArea = int('100')
        self.maxArea = int('2000')
        self.minLegLength = int('11')
        self.maxLegLength = int('80')
        self.legRatio = float('2.1')
        self.colorCorr = True
        self.baseTriangleCount = int('280')
        self.polyApproxFactor = float('4')

class TriRecognizer:
    def __init__(self):
        self.__reset()

    def __reset(self):
        self._rawContourCount = 0
        self._rawTriCount = 0
        self._boundTriCount = 0
        self._arclenTriCount = 0
        self._areaTriCount = 0
        self._legLengthTriCount = 0
        self._legVarTriCount = 0
        self._finalTriCount = 0

        self.minArea = sys.maxsize
        self.maxArea = 0
        self.minArcL = sys.maxsize
        self.maxArcL = 0
        self.minLeg = sys.maxsize
        self.maxLeg = 0
        self.maxRatio = 0

    def processImage(self, srcImageFile, params):
        self.__reset()

        # read source
        image = self.prepareSourceImage(srcImageFile, params)

        # detect triangles
        triList = self.findTriangles(image, params)

        fullPercent = 1.0 - float(self._finalTriCount) / float(params.baseTriangleCount)

        # build output dictionary
        bounds = []
        if len(params.bounds) > 0:
            bounds = params.bounds.tolist()

        # if distortCoeffs != []:
        distortCoeffs = params.distortCoeffs.flatten()
        distortCoeffs = params.distortCoeffs.tolist()

        imgheight, imgwidth = image.shape[:2]

        outDict = {'Parameters': {
            'TrianglesExpected': params.baseTriangleCount,
            'MinArcLength': params.minArcLength,
            'MaxArcLength': params.maxArcLength,
            'MinTriangleArea': params.minArea,
            'MaxTriangleArea': params.maxArea,
            'MinLegLength': params.minLegLength,
            'MaxLegLength': params.maxLegLength,
            'MaxLegVariation': params.maxLegVar,
            'MaxLegRatio': params.legRatio,
            'MaxHeightRatio': params.heightLegRatio,
            'PolygonApproximationFactor': params.polyApproxFactor,
            'UseAdaptiveThreshold': params.useThreshold,
            'SharpenImage': params.useSharpen,
            'UseBoundingPolygon': str(params.useBounds),
            'BoundingPolygon': bounds,
            'UndistortImage': str(params.useDistort),
            'UndistortCoeffs': distortCoeffs,
            'UseStaticThreshold': params.useStaticThreshold,
            'ColorCorrection': params.colorCorr,
            'StaticThreshold': params.staticThreshold,
            'EqualizeHistogram': str(params.equalizeHist)},
            'DetectionDetails': {
                'ContourCount': self._rawContourCount,
                'RawTriangleCount': self._rawTriCount,
                'InBoundingPolyCount': self._boundTriCount,
                'CorrectArclenTriangleCount': self._arclenTriCount,
                'CorrectAreaTriangleCount': self._areaTriCount,
                'CorrectLegLengthTriangleCount': self._legLengthTriCount,
                'CorrectLegVarTriangleCount': self._legVarTriCount,
                'ImageWidth': imgwidth,
                'ImageHeight': imgheight,
                'TriangleCoords': triList
            },
            'TriangleCount': self._finalTriCount,
            'PercentFull': fullPercent
        }
        return outDict
        # write output JSON to STDOUT
        # json.dump(outDict, sys.stdout, indent=4)
        # --> json.dump(outDict, sys.stdout)
        # --> sys.stdout.write('\n')
        # sys.stdout.write(str('\nTrangles Count: '+str(outDict['TriangleCount'])+'\nPercent: '+str(outDict['PercentFull'])))
        # sys.stdout.write(str(outDict))



    @staticmethod
    def __rmShadow(img):
        rgb_planes = cv2.split(img)

        result_planes = []
        result_norm_planes = []
        for plane in rgb_planes:
            # dilated_img = cv2.dilate(plane, np.ones((7, 7), np.uint8))
            dilated_img = cv2.dilate(plane, np.ones((3, 3), np.uint8))
            # dilated_img = cv2.dilate(plane, np.ones((17, 17), np.uint8))
            # bg_img = cv2.medianBlur(dilated_img, 21)
            bg_img = cv2.medianBlur(dilated_img, 95)
            # bg_img = cv2.medianBlur(dilated_img, 95)
            diff_img = 255 - cv2.absdiff(plane, bg_img)
            # norm_img = cv2.normalize(diff_img, None, alpha=0, beta=255, norm_type=cv2.NORM_MINMAX, dtype=cv2.CV_8UC1)
            result_planes.append(diff_img)
            # result_norm_planes.append(norm_img)

        img = cv2.merge(result_planes)
        # img = cv2.merge(result_norm_planes)

        return img

    @staticmethod
    def __colorCorrection(img):
        _, _, img = cv2.split(img)
        return img

    @staticmethod
    def prepareSourceImage(srcImageFile, params):
        # read input file
        img = cv2.imread(srcImageFile)
        # write state image
        if params.outputState == True:
            cv2.imwrite('state_01_input.jpg', img)

        if params.useDistort == True:
            img = TriRecognizer.__undistortImage(img, params.distortCoeffs, params.outputState)

        # sharpen
        if params.useSharpen == True:
            kernel = np.array([[-1, -1, -1], [-1, 9, -1], [-1, -1, -1]])
            img = cv2.filter2D(img, -1, kernel)
            # write state image
            if params.outputState == True:
                cv2.imwrite('state_03_sharpen.jpg', img)

        # convert to grayscale
        if params.colorCorr:
            img = TriRecognizer.__colorCorrection(img)
        else:
            img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        # write state image
        if params.outputState == True:
            cv2.imwrite('state_04_grayscale.jpg', img)

        # equalize histogram
        if params.equalizeHist == True:
            img = cv2.equalizeHist(img)
            # write state image
            if params.outputState == True:
                cv2.imwrite('state_05_histogramequalization.jpg', img)

        # apply adaptive threshold to image
        if params.useThreshold == True:
            img = cv2.adaptiveThreshold(img, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 19, 15)

            # write state image
            if params.outputState == True:
                cv2.imwrite('state_06_adaptivethreshold.jpg', img)

        # apply static threshold to image
        if params.useStaticThreshold == True:
            _, img = cv2.threshold(img, params.staticThreshold, 255, cv2.THRESH_BINARY)
            # write state image
            if params.outputState == True:
                cv2.imwrite('state_07_staticthreshold.jpg', img)

        return img

    @staticmethod
    def __undistortImage(img, coeffs, outputState):
        camMatrix = np.eye(3, dtype=np.float32)

        w = img.shape[1]
        h = img.shape[0]
        camMatrix[0, 2] = w / 2.0  # width
        camMatrix[1, 2] = h / 2.0  # height
        camMatrix[0, 0] = 10.0
        camMatrix[1, 1] = 10.0

        dim = (w, h)
        newcameramtx, roi = cv2.getOptimalNewCameraMatrix(camMatrix, coeffs, dim, 1, dim)
        mapx, mapy = cv2.initUndistortRectifyMap(camMatrix, coeffs, None, newcameramtx, dim, 5)
        img = cv2.remap(img, mapx, mapy, cv2.INTER_LINEAR)

        # write state image
        if outputState == True:
            cv2.imwrite('state_02_undistort.jpg', img)

        return img

    @staticmethod
    def __checkBounds(shape, usebounds, bounds):
        if usebounds == False:
            return True

        for pt in shape:
            if cv2.pointPolygonTest(bounds, (pt[0][0], pt[0][1]), False) < 1:
                return False

        return True

    @staticmethod
    def __segLen(x1, y1, x2, y2):
        deltax = x2 - x1
        deltay = y2 - y1
        return int(math.sqrt(deltax * deltax + deltay * deltay))

    @staticmethod
    def __pointDistance(lx1, ly1, lx2, ly2, px, py):
        s = (ly2 - ly1) * px - (lx2 - lx1) * py + lx2 * ly1 - ly2 * lx1
        return abs(s / TriRecognizer.__segLen(lx1, ly1, lx2, ly2))

    @staticmethod
    def __drawTriangle(img, shape, color=(0, 255, 0), lineWidth=4):
        cv2.line(img, (shape[0][0][0], shape[0][0][1]), (shape[1][0][0], shape[1][0][1]), color, lineWidth)
        cv2.line(img, (shape[1][0][0], shape[1][0][1]), (shape[2][0][0], shape[2][0][1]), color, lineWidth)
        cv2.line(img, (shape[2][0][0], shape[2][0][1]), (shape[0][0][0], shape[0][0][1]), color, lineWidth)

    def __checkTriangleGeometry(self, shape, params, images, stateColor, stateLineWidth):
        arcLength = cv2.arcLength(shape, True)

        if params.minArcLength < arcLength < params.maxArcLength:
            self._arclenTriCount += 1

            if params.outputState:
                self.__drawTriangle(images[0], shape, stateColor, stateLineWidth)

            area = cv2.contourArea(shape)

            if params.minArea < area < params.maxArea:
                self._areaTriCount += 1
                # triName="tri" + str(self._finalTriCount)
                # triList[triName]={"x1":int(shape[0][0][0]),"y1":shape[0][0][1],"x2":shape[1][0][0],"y2":shape[1][0][1],"x3":shape[2][0][0],"y3":shape[2][0][1]}

                if params.outputState:
                    self.__drawTriangle(images[1], shape, stateColor, stateLineWidth)

                leglen1 = TriRecognizer.__segLen(shape[0][0][0], shape[0][0][1], shape[1][0][0], shape[1][0][1])
                leglen2 = TriRecognizer.__segLen(shape[1][0][0], shape[1][0][1], shape[2][0][0], shape[2][0][1])
                leglen3 = TriRecognizer.__segLen(shape[2][0][0], shape[2][0][1], shape[0][0][0], shape[0][0][1])

                if leglen1 > params.minLegLength and leglen2 > params.minLegLength and leglen3 > params.minLegLength and \
                        leglen1 < params.maxLegLength and leglen2 < params.maxLegLength and leglen3 < params.maxLegLength:
                    self._legLengthTriCount += 1

                    if params.outputState:
                        self.__drawTriangle(images[2], shape, stateColor, stateLineWidth)

                    legDiff = False
                    legRatio1 = max(leglen1, leglen2) / min(leglen1, leglen2)
                    legRatio2 = max(leglen1, leglen3) / min(leglen1, leglen3)
                    legRatio3 = max(leglen3, leglen2) / min(leglen3, leglen2)
                    if params.legRatio >= 0:
                        if legRatio1 < params.legRatio and legRatio2 < params.legRatio and legRatio3 < params.legRatio:
                            legDiff = True
                    else:
                        if abs(leglen1 - leglen2) < params.maxLegVar and \
                                abs(leglen2 - leglen3) < params.maxLegVar and \
                                abs(leglen1 - leglen3) < params.maxLegVar:
                            legDiff = False

                    if legDiff:
                        self._legVarTriCount += 1

                        if params.outputState:
                            self.__drawTriangle(images[3], shape, stateColor, stateLineWidth)

                        h1 = self.__pointDistance(shape[0][0][0], shape[0][0][1],
                                                  shape[1][0][0], shape[1][0][1],
                                                  shape[2][0][0], shape[2][0][1])
                        h2 = self.__pointDistance(shape[1][0][0], shape[1][0][1],
                                                  shape[2][0][0], shape[2][0][1],
                                                  shape[0][0][0], shape[0][0][1])
                        h3 = self.__pointDistance(shape[2][0][0], shape[2][0][1],
                                                  shape[0][0][0], shape[0][0][1],
                                                  shape[1][0][0], shape[1][0][1])

                        # checking triangle heights
                        if (leglen2 / h1) < params.heightLegRatio and \
                                (leglen3 / h1) < params.heightLegRatio and \
                                (leglen1 / h2) < params.heightLegRatio and \
                                (leglen3 / h2) < params.heightLegRatio and \
                                (leglen1 / h3) < params.heightLegRatio and \
                                (leglen2 / h3) < params.heightLegRatio:

                            if params.outputState:
                                self.__drawTriangle(images[4], shape, stateColor, stateLineWidth)

                            self.minArea = min(self.minArea, area)
                            self.maxArea = max(self.maxArea, area)
                            self.minArcL = min(self.minArcL, arcLength)
                            self.maxArcL = max(self.maxArcL, arcLength)
                            self.minLeg = min(self.minLeg, leglen1, leglen2, leglen3)
                            self.maxLeg = max(self.maxLeg, leglen1, leglen2, leglen3)
                            self.maxRatio = max(self.maxRatio, legRatio1, legRatio2, legRatio3)
                            return True
        return False

    def findTriangles(self, img, params):
        triList = []
        tmpTriList = []
        stateColor = (0, 255, 0)
        stateBoundColor = (0, 0, 255)
        stateLineWidth = 4
        imgContours = None
        imgPAF = None
        imgRawTris = None
        imgBoundedTris = None
        imgArcLen = None
        imgArea = None
        imgLegLength = None
        imgLegVar = None
        imgCrossTris = None
        imgHeight = None

        if params.outputState == True:
            imgOutput = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)

            imgBounds = imgOutput.copy()
            if params.useBounds == True:
                cv2.line(imgBounds, (params.bounds[0][0][0], params.bounds[0][0][1]),
                         (params.bounds[0][1][0], params.bounds[0][1][1]),
                         stateBoundColor, stateLineWidth)
                cv2.line(imgBounds, (params.bounds[0][1][0], params.bounds[0][1][1]),
                         (params.bounds[0][2][0], params.bounds[0][2][1]),
                         stateBoundColor, stateLineWidth)
                cv2.line(imgBounds, (params.bounds[0][2][0], params.bounds[0][2][1]),
                         (params.bounds[0][3][0], params.bounds[0][3][1]),
                         stateBoundColor, stateLineWidth)
                cv2.line(imgBounds, (params.bounds[0][3][0], params.bounds[0][3][1]),
                         (params.bounds[0][0][0], params.bounds[0][0][1]),
                         stateBoundColor, stateLineWidth)

            imgContours = imgOutput.copy()
            imgPAF = imgOutput.copy()
            imgRawTris = imgOutput.copy()
            imgBoundedTris = imgBounds.copy()
            imgArcLen = imgOutput.copy()
            imgArea = imgOutput.copy()
            imgLegLength = imgOutput.copy()
            imgLegVar = imgOutput.copy()
            imgCrossTris = imgOutput.copy()
            imgHeight = imgOutput.copy()

        _, contours, _ = cv2.findContours(img, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)

        if params.outputState == True:
            cv2.drawContours(imgContours, contours, -1, stateColor, stateLineWidth)

        for i in range(0, len(contours)):
            self._rawContourCount += 1
            shape = cv2.approxPolyDP(contours[i], params.polyApproxFactor, True)

            if params.outputState == True:
                outShape = [shape]
                cv2.drawContours(imgPAF, outShape, -1, stateColor, stateLineWidth)

            if len(shape) == 3:
                if params.outputState == True:
                    self.__drawTriangle(imgRawTris, shape, stateColor, stateLineWidth)

                self._rawTriCount += 1
                if TriRecognizer.__checkBounds(shape, params.useBounds, params.bounds) == True:
                    self._boundTriCount += 1

                    if params.outputState == True:
                        self.__drawTriangle(imgBoundedTris, shape, stateColor, stateLineWidth)

                    if self.__checkTriangleGeometry(shape, params,
                                                    (imgArcLen, imgArea, imgLegLength, imgLegVar, imgHeight),
                                                    stateColor, stateLineWidth):
                        tmpTriList.append(shape)

        # remove crossed or internal triangles
        tmpTriList = np.array(tmpTriList, np.int32)
        for shape in tmpTriList:
            cross = False
            for bound in tmpTriList:
                if TriRecognizer.__checkBounds(shape, True, bound):
                    cross = True
                    break;

            if not cross:
                self._finalTriCount += 1
                triList.append([[int(shape[0][0][0]), int(shape[0][0][1])],
                                [int(shape[1][0][0]), int(shape[1][0][1])],
                                [int(shape[2][0][0]), int(shape[2][0][1])]])

                if params.outputState == True:
                    TriRecognizer.__drawTriangle(imgCrossTris, shape, stateColor, stateLineWidth)

        # write state images
        if params.outputState == True:
            cv2.imwrite('state_08_detectedcontours.jpg', imgContours)
            cv2.imwrite('state_09_polyapproxfactor.jpg', imgPAF)
            cv2.imwrite('state_10_rawtriangles.jpg', imgRawTris)
            cv2.imwrite('state_11_boundedtriangles.jpg', imgBoundedTris)
            cv2.imwrite('state_12_arclength.jpg', imgArcLen)
            cv2.imwrite('state_13_area.jpg', imgArea)
            cv2.imwrite('state_14_leglength.jpg', imgLegLength)
            cv2.imwrite('state_15_legvar.jpg', imgLegVar)
            cv2.imwrite('state_16_height.jpg', imgHeight)
            cv2.imwrite('state_17_cross.jpg', imgCrossTris)

        return triList
