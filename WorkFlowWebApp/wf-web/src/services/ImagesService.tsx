import { MinMaxDatesDto } from '../model/MinMaxDatesDto'
import axios, { AxiosInstance } from 'axios';
import MomentTimezone from 'moment-timezone';
import { ServiceConfig } from './api.config'
import { Moment } from 'moment-timezone';
import { ApplicationEvent } from '../redux/Actions';
import { ImageKeyDto, ImageDto, toJsonImageDto } from '../model/ImageDto';


export interface ImagesService {

    getNextImage(url: string): Promise<string>;
    getPrevImage(url: string): Promise<string>;
    getImage(url: string): Promise<string>;
    saveImage(url: string, img: ImageDto): Promise<string>;

    getLastImages(pageNumber: number): Promise<string>;

    getPageOfImages(url: string): Promise<string>;

    getImagesByDate(
        min: Moment,
        max: Moment,
        intervalType: string): Promise<string>;
}

export default class ImagesServiceImpl implements ImagesService {

    axiosInstance: AxiosInstance;
    baseUrlToGetImagesPerDate: string;
    urlToGetLastImages: string;


    protected buildRow(rowName: string, creationDate: Moment, version: number, imageId: string): string {
        var params: { [key: string]: string } = {
            creationDate: encodeURIComponent(creationDate.format('YYYY-MM-DD HH:mm:ss +01')),
            version: encodeURIComponent(version),
            imageId: encodeURIComponent(imageId)
        };
        var row = Object.keys(params).map(key => key + ',' + params[key]).join(',');
        return rowName + '=' + row;
    }

    protected buildParam(paramName: string, paramValue: string) {
        return paramName + '=' + encodeURIComponent(paramValue);
    }

    protected buildSort(sorted: string, unsorted: string, empty: string) {
        var params: { [key: string]: string } = {
            sorted: sorted,
            unsorted: unsorted,
            empty: empty
        };
        var row = Object.keys(params).map(key => key + ',' + encodeURIComponent(params[key])).join(',');
        return 'sort=' + row;
    }

    protected buildURLToGetDates(min: Moment, max: Moment, intervalType: string): string {
        var getParams: { [key: string]: string } = {
            firstRow: this.buildRow('firstRow', MomentTimezone(0), 0, ' '),
            lastRow: this.buildRow('lastRow', MomentTimezone(0), 0, ' '),
            pageNumber: this.buildParam('pageNumber', '0'),
            pageSize: this.buildParam('pageSize', '100'),
            sort: this.buildSort('true', 'true', 'true'),
            offset: this.buildParam('offset', '100'),
            paged: this.buildParam('paged', 'true'),
            unpaged: this.buildParam('paged', 'true')
        };

        var queryParams = Object.keys(getParams).map(key => getParams[key]).join('&');

        var params: { [key: string]: string } = {
            intervalType: intervalType,
            firstDate: encodeURIComponent(min.format('YYYY-MM-DD HH:mm:ss +01')),
            lastDate: encodeURIComponent(max.format('YYYY-MM-DD HH:mm:ss +01')),
            version: '0'
        };
        var queryString = Object.keys(params).map(key => params[key]).join('/');
        return this.baseUrlToGetImagesPerDate + queryString + '?' + queryParams;
    }

    protected buildURLToGetLastImages(pageNumber: number) {
        var getParams: { [key: string]: string } = {
            firstRow: this.buildRow('firstRow', MomentTimezone(0), 0, ' '),
            lastRow: this.buildRow('lastRow', MomentTimezone(0), 0, ' '),
            pageNumber: this.buildParam('pageNumber', pageNumber.toString()),
            page: this.buildParam('page', pageNumber.toString()),
            pageSize: this.buildParam('pageSize', '100'),
            sort: this.buildSort('true', 'true', 'true'),
            offset: this.buildParam('offset', '100'),
            paged: this.buildParam('paged', 'true'),
            unpaged: this.buildParam('paged', 'true')
        };

        var queryParams = Object.keys(getParams).map(key => getParams[key]).join('&');
        return this.urlToGetLastImages + '?' + queryParams;

    }


    constructor() {
        this.axiosInstance = axios.create(ServiceConfig);
        this.baseUrlToGetImagesPerDate = '/api/gs/images/odt/';
        this.urlToGetLastImages = '/api/gs/images';
    }

    async getImagesByDate(
        min: Moment,
        max: Moment,
        intervalType: string): Promise<string> {
        const urlToGetDates = this.buildURLToGetDates(min, max, intervalType);
        return this.axiosInstance.get(urlToGetDates)
            .then(resp => resp.data);
    }

    async saveImage(url: string, img: ImageDto): Promise<string> {
        return this.axiosInstance.post(url, toJsonImageDto(img), {
            headers: {
                'Content-Type': 'application/json'
            }
        })
            .then(resp => resp.data);
    }


    async getLastImages(pageNumber: number) {
        const url = this.buildURLToGetLastImages(pageNumber);
        console.log("Calling url to get last images : " + url);
        return this.axiosInstance.get(url)
            .then(resp => resp.data);

    }

    async getPageOfImages(url: string): Promise<string> {
        return this.axiosInstance.get(url)
            .then(resp => resp.data);
    }

    async getNextImage(url: string): Promise<string> {
        return this.axiosInstance.get(url)
            .then(resp => resp.data);
    }
    async getPrevImage(url: string): Promise<string> {
        return this.axiosInstance.get(url)
            .then(resp => resp.data);
    }

    async getImage(url: string): Promise<string> {
        return this.axiosInstance.get(url)
            .then(resp => resp.data);
    }


}








